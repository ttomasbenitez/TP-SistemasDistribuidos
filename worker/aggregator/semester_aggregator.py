from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
from Middleware.connection import PikaConnection
import os
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT
from utils.heartbeat import start_heartbeat_sender
import threading
from pkg.storage.state_storage.semester_agg import SemesterAggregatorStateStorage

class SemesterAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 eof_output_exchange: str,
                 eof_output_queues: dict, 
                 eof_self_queue: str,
                 eof_service_queue: str,
                 host: str):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        
        self.connection = PikaConnection(host)
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue
        # Track last seen message number per upstream node (store_aggregator)
        self._last_msg_by_sender = {}
        self._sender_lock = threading.Lock()
        # In-memory aggregation by request_id -> period -> store_id -> total
        self._agg_by_request = {}
        # Persistent storage
        storage_dir = os.getenv('SEMESTER_STORAGE_DIR', './data/semester_agg')
        self.state_storage = SemesterAggregatorStateStorage(storage_dir)

    def _should_process_and_update(self, message: Message) -> bool:
        """
        Decide if message should be processed based on per-sender last msg_num.
        - If equal to last -> duplicate (skip)
        - If greater than last -> accept and update
        - If less than last -> out-of-order (skip)
        """
        sender_id = message.get_node_id()
        with self._sender_lock:
            last = self._last_msg_by_sender.get(sender_id, -1)
            if message.msg_num == last:
                logging.info(f"action: duplicate_msg | sender: {sender_id} | msg_num: {message.msg_num} | decision: skip")
                return False
            if message.msg_num < last:
                logging.warning(f"action: out_of_order_msg | sender: {sender_id} | msg_num: {message.msg_num} | last: {last} | decision: skip")
                return False
            # message.msg_num > last → accept
            self._last_msg_by_sender[sender_id] = message.msg_num
            return True

    def start(self):
        
        self.heartbeat_sender = start_heartbeat_sender()

        self.connection.start()
        self._consume_data_queue()
        # self._consume_eof()
        self.connection.start_consuming()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        eof_output_exchange = MessageMiddlewareExchange(self.eof_output_exchange, self.eof_output_queues, self.connection)
        # eof_self_queue = MessageMiddlewareQueue(self.eof_self_queue, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_queue, eof_output_exchange])
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    # On EOF: emit all aggregated results for this request_id, then send EOF downstream and cleanup
                    self._emit_all_and_finalize(message, data_output_queue)
                    return
                
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id) 

                # Load persisted state on first sight of request_id
                if message.request_id not in self._agg_by_request:
                    self._agg_by_request[message.request_id] = {}
                    # Load persisted state and merge
                    self.state_storage.load_state(message.request_id)
                    persisted = self.state_storage.data_by_request.get(message.request_id, {})
                    persisted_agg = persisted.get("agg", {})
                    persisted_last = persisted.get("last_msg_by_sender", {})
                    # Merge aggregation
                    for period, store_map in persisted_agg.items():
                        bucket = self._agg_by_request[message.request_id].setdefault(period, {})
                        for store_id, total in store_map.items():
                            bucket[store_id] = bucket.get(store_id, 0.0) + total
                    # Merge last seen
                    with self._sender_lock:
                        for sender_id, last_num in persisted_last.items():
                            self._last_msg_by_sender[sender_id] = max(self._last_msg_by_sender.get(sender_id, -1), last_num)

                # Per-sender sequencing check
                if not self._should_process_and_update(message):
                    return

                items = message.process_message()
                # Aggregate in-memory and persist incremental delta
                per_request = self._agg_by_request[message.request_id]
                deltas = []  # list of (period, store_id, delta)
                store_id = None
                for it in items:
                    year = it.get_year()
                    sem  = it.get_semester()
                    period = f"{year}-H{sem}"
                    if store_id is None:
                        store_id = it.store_id
                    amount = it.get_final_amount()
                    bucket = per_request.setdefault(period, {})
                    bucket[store_id] = bucket.get(store_id, 0.0) + amount
                    deltas.append((period, store_id, amount))

                # Persist incremental changes and last seen msg for sender
                sender_id = message.get_node_id()
                lines = []
                for period, s_id, delta in deltas:
                    lines.append(f"agg;{period};{s_id};{delta}")
                lines.append(f"sender;{sender_id};{message.msg_num}")
                self.state_storage.data_by_request[message.request_id] = {"lines": lines}
                self.state_storage.save_state(message.request_id)

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
        
        data_input_queue.start_consuming(__on_message__)

    def _emit_all_and_finalize(self, message: Message, data_output_queue: MessageMiddlewareQueue):
        """Emit all aggregated Q3 intermediate results and then EOF downstream."""
        request_id = message.request_id
        try:
            per_request = self._agg_by_request.get(request_id, {})
            for period, store_map in per_request.items():
                for store_id, total in store_map.items():
                    res = Q3IntermediateResult(period, store_id, total)
                    self._send_grouped_item(message, res, data_output_queue)
        finally:
            # Send EOF to downstream exchange for q3
            data_output_queue.send(message.serialize(), str(message.type))
            # Cleanup state both memory and disk
            try:
                del self._agg_by_request[request_id]
            except Exception:
                pass
            self.state_storage.delete_state(request_id)
   
    def _send_grouped_item(self, message, item, data_output_queue):
        new_chunk = item.serialize()
        new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, message.msg_num, new_chunk)
        data_output_queue.send(new_message.serialize())

def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue": os.getenv('OUTPUT_QUEUE_1'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_2'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    eof_exchange_queues =  {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                                 config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    
    aggregator = SemesterAggregator(config_params["input_queue"], 
                                    config_params["output_queue"], 
                                    config_params["eof_exchange_name"], 
                                    eof_exchange_queues,
                                    config_params["eof_self_queue"],
                                    config_params["eof_service_queue"],  
                                    config_params["rabbitmq_host"])
    aggregator.start()

if __name__ == "__main__":
    main()
