from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.q1_result import Q1Result
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_1_RESULT
from multiprocessing import Process
from utils.heartbeat import start_heartbeat_sender

MAX_PENDING_SIZE = 1000
AMOUNT_THRESHOLD = 75

class FilterAmountNode(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 eof_output_exchange: str, 
                 eof_output_queues: str,
                 eof_self_queue: str, 
                 eof_service_queue: str, 
                 eof_final_queue: str, 
                 host: str, 
                 amount_to_filter: int,
                 total_shards: int):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.host = host
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_self_queue = eof_self_queue
        self.eof_service_queue = eof_service_queue
        self.eof_final_queue = eof_final_queue
        self.clients = []
        self.amount_to_filter = amount_to_filter
        self.total_shards = total_shards
        
    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF FINAL process")
        p_eof_final = Process(target=self._consume_eof_final)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof_final): p.start()
        for p in (p_data, p_eof_final): p.join()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        eof_output_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_input_queue, data_output_exchange, eof_output_exchange])
        
        # Diccionario para guardar request_id -> last_contiguous_msg_num
        self.last_contiguous_msg_num = {}
        # Diccionario para guardar request_id -> set(pending_messages)
        self.pending_messages = {}

        def __on_message__(message_body):
            try:
                message = Message.deserialize(message_body)

                if message.type == MESSAGE_TYPE_EOF:
                    self._handle_eof(message, eof_output_exchange)
                    return

                self._handle_message(message, data_output_exchange)

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
                raise e

        data_input_queue.start_consuming(__on_message__, manual_ack=False)

    def _handle_eof(self, message, eof_output_exchange):
        logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
        # Send EOF to service queue
        eof_output_exchange.send(message.serialize(), str(message.type))
        
        # Limpiar estado de duplicados para este request_id
        if message.request_id in self.last_contiguous_msg_num:
            del self.last_contiguous_msg_num[message.request_id]
        if message.request_id in self.pending_messages:
            del self.pending_messages[message.request_id]
        
        logging.info(f"action: ACK EOF")

    def _handle_message(self, message, data_output_exchange):
        self._initialize_request_state(message.request_id)
        
        if self._is_duplicate(message):
            return

        self.pending_messages[message.request_id].add(message.msg_num)
        self._clean_window_if_needed(message.request_id)
        
        # Procesar mensaje
        logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type} | msg_num: {message.msg_num}")
        self._ensure_request(message.request_id)
        self._inc_inflight(message.request_id)

        self._process_and_send_items(message, data_output_exchange)
        
        self._dec_inflight(message.request_id)

        self._update_contiguous_sequence(message)

    def _initialize_request_state(self, request_id):
        if request_id not in self.last_contiguous_msg_num:
            self.last_contiguous_msg_num[request_id] = 0
            self.pending_messages[request_id] = set()

    def _is_duplicate(self, message):
        last_cont = self.last_contiguous_msg_num[message.request_id]
        
        if message.msg_num <= last_cont:
            logging.info(f"action: Duplicate message received | request_id: {message.request_id} | msg_num: {message.msg_num} | last_contiguous: {last_cont}")
            return True
        
        if message.msg_num in self.pending_messages[message.request_id]:
             logging.info(f"action: Duplicate pending message received | request_id: {message.request_id} | msg_num: {message.msg_num}")
             return True
             
        return False

    def _clean_window_if_needed(self, request_id):
        if len(self.pending_messages[request_id]) > MAX_PENDING_SIZE:
            logging.info(f"action: Clearing pending messages window | request_id: {request_id} | size: {len(self.pending_messages[request_id])}")
            self.pending_messages[request_id].clear()

    def _update_contiguous_sequence(self, message):
        last_cont = self.last_contiguous_msg_num[message.request_id]
        prev_expected = message.msg_num - self.total_shards
        
        if prev_expected <= last_cont:
            self.last_contiguous_msg_num[message.request_id] = message.msg_num
            self.pending_messages[message.request_id].remove(message.msg_num)
            
            current_check = message.msg_num + self.total_shards
            while current_check in self.pending_messages[message.request_id]:
                self.last_contiguous_msg_num[message.request_id] = current_check
                self.pending_messages[message.request_id].remove(current_check)
                current_check += self.total_shards

    def _process_and_send_items(self, message, data_output_exchange):
        items = message.process_message()
        if items:
            new_chunk = '' 
            for item in items:
                amount = item.get_final_amount()
                if amount >= self.amount_to_filter:
                    new_chunk += Q1Result(item.transaction_id, amount).serialize()
            if new_chunk:
                new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_1_RESULT, message.msg_num, new_chunk)
                serialized = new_message.serialize()
                data_output_exchange.send(serialized, str(message.request_id))
        
    def _consume_eof_final(self):
        eof_final_queue = MessageMiddlewareQueue(self.host, self.eof_final_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        self.message_middlewares.extend([eof_final_queue, data_output_exchange])
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: final EOF message received | request_id: {message.request_id}")   
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    data_output_exchange.send(message.serialize(), str(message.request_id))
            except Exception as e:
                logging.error(f"action: ERROR processing final EOF message | error: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

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
        "exchange": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
        "total_shards": int(os.getenv('TOTAL_SHARDS', 3)),
    }
    
    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "exchange",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    # For sharding, we send EOF directly to the service queue
    eof_queues = {config_params["eof_service_queue"]: [str(MESSAGE_TYPE_EOF)]}

    filter = FilterAmountNode(config_params["input_queue"], 
                            config_params["exchange"], 
                            config_params["eof_exchange_name"],
                            eof_queues,
                            config_params["eof_self_queue"],
                            config_params["eof_service_queue"],
                            config_params["eof_final_queue"],
                            config_params["rabbitmq_host"],  
                            AMOUNT_THRESHOLD,
                            config_params["total_shards"])
    filter.start()

if __name__ == "__main__":
    main()
