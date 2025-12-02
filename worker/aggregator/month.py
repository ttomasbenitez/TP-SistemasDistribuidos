from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from Middleware.connection import PikaConnection
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
from pkg.message.q2_result import Q2IntermediateResult
import os
from multiprocessing import Process, Value
from utils.heartbeat import start_heartbeat_sender
import hashlib
from pkg.message.utils import calculate_sub_message_id
from pkg.message.constants import SUB_MESSAGE_START_ID

BATCH_SIZE = 100


class AggregatorMonth(Worker):
    
    def __init__(self,
                 data_input_queue: str,
                 data_output_queues: list,
                 eof_output_exchange: str,
                 eof_output_queues: dict,
                 eof_self_queue: str,
                 eof_service_queue: str,
                 host: str):
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.data_output_queues = data_output_queues
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.connection = PikaConnection(host)
        self.eof_self_queue= eof_self_queue
        self.eof_service_queue = eof_service_queue
    
    def start(self):
        # logging.info(f"Starting process")
        # p_data = Process(target=self._consume_data_queue)
        
        # logging.info(f"Starting EOF node process")
        # p_eof = Process(target=self._consume_eof)
        
        # self.heartbeat_sender = start_heartbeat_sender()

        # for p in (p_data, p_eof): p.start()
        # for p in (p_data, p_eof): p.join()
        self.connection.start()
        self._consume_data_queue()
        self._consume_eof()
        self.connection.start_consuming()
       
    
    def _consume_data_queue(self):
        eof_output_exchange = MessageMiddlewareExchange(self.eof_output_exchange, self.eof_output_queues, self.connection)
        data_output_queues = [MessageMiddlewareQueue(queue, self.connection) for queue in self.data_output_queues]
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.extend([eof_output_exchange, data_input_queue] + data_output_queues)
        logging.info(f"COLASSS {data_output_queues}")
        self.buffers = {}
        self.last_message = {}

        def _flush_buffer(request_id, month, output_queues):
            if request_id not in self.buffers or month not in self.buffers[request_id]:
                return
            
            items = self.buffers[request_id][month]
            if not items:
                return

            # Use the last message as a template
            original_message = self.last_message.get(request_id)
            if not original_message:
                logging.error(f"action: flush_buffer | error: no original message found for request_id: {request_id}")
                return

            single_group = {month: items}
            
            template_message = Message(original_message.request_id, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT, original_message.msg_num, '')
            
            self._send_groups_sharded(template_message, single_group, output_queues)
            
            # Clear the buffer
            self.buffers[request_id][month] = []

        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    
                    # Flush all remaining buffers for this request_id
                    if message.request_id in self.buffers:
                        for month in list(self.buffers[message.request_id].keys()):
                            _flush_buffer(message.request_id, month, data_output_queues)
                        del self.buffers[message.request_id]
                    
                    if message.request_id in self.last_message:
                        del self.last_message[message.request_id]

                    eof_output_exchange.send(message.serialize(), str(message.type))
                    return

                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)

                self.last_message[message.request_id] = message
                if message.request_id not in self.buffers:
                    self.buffers[message.request_id] = {}

                items = message.process_message()
                groups = self._group_items_by_month(items)
                
                for month, group_items in groups.items():
                    if month not in self.buffers[message.request_id]:
                        self.buffers[message.request_id][month] = []
                    
                    self.buffers[message.request_id][month].extend(group_items)
                    
                    if len(self.buffers[message.request_id][month]) >= BATCH_SIZE:
                        _flush_buffer(message.request_id, month, data_output_queues)

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
            
        data_input_queue.start_consuming(__on_message__)

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q2IntermediateResult(f"{year}-0{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    
    def _send_groups_sharded(self, original_message: Message, groups: dict, output_queues: list):
        sub_msg_id = SUB_MESSAGE_START_ID
        for key, items in groups.items():
            # key is "YYYY-MM"
            # Use hash to select queue
            hash_val = int(hashlib.sha256(key.encode()).hexdigest(), 16)
            queue_index = hash_val % len(output_queues)
            target_queue = output_queues[queue_index]
            new_chunk = ''.join(item.serialize() for item in items)
            new_msg_num = calculate_sub_message_id(original_message.msg_num, sub_msg_id)
            new_message = original_message.new_from_original(new_chunk, msg_num=new_msg_num)
            logging.info(f"action: sending group | key: {key} | to_queue_index: {queue_index} | request_id: {new_message.request_id} | type: {new_message.type}")
            serialized = new_message.serialize()
            target_queue.send(serialized)
            sub_msg_id += 1

    def close(self):
        try:
            for middleware in self.message_middlewares:
                middleware.close()
            self.heartbeat_sender.stop()
            self.message_middlewares = []
            logging.info(f"action=close_connections | status=success")
        except Exception as e:
            logging.error(f"Error al cerrar las conexiones: {type(e).__name__}: {e}")

def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["eof_self_queue"] = os.getenv('EOF_SELF_QUEUE')
    
    # Read multiple output queues
    output_queues = []
    i = 1
    while True:
        queue = os.getenv(f'OUTPUT_QUEUE_{i}')
        if not queue:
            break
        output_queues.append(queue)
        i += 1
    
    if not output_queues:
        # Fallback to OUTPUT_QUEUE_1 if loop didn't find anything (though loop starts at 1)
        # Actually if OUTPUT_QUEUE_1 is missing it breaks immediately.
        # Let's check if we found any.
        pass

    config_params["output_queues"] = output_queues
    config_params["eof_exchange_name"] = os.getenv('EOF_EXCHANGE_NAME')
    config_params["eof_queue_1"] = os.getenv('EOF_QUEUE_1')
    config_params["eof_queue_2"] = os.getenv('EOF_QUEUE_2')
    config_params["eof_service_queue"] = os.getenv('EOF_SERVICE_QUEUE')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["logger_name"] = os.getenv('CONTAINER_NAME')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or not config_params["output_queues"]:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    eof_output_queues = {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                            config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    
    logging.info(f"Initialized output queuques: {config_params['output_queues']}")

    aggregator = AggregatorMonth(config_params["input_queue"], 
                                config_params["output_queues"], 
                                config_params["eof_exchange_name"], 
                                eof_output_queues,
                                config_params["eof_self_queue"],
                                config_params["eof_service_queue"],
                                config_params["rabbitmq_host"])
    aggregator.start()

if __name__ == "__main__":
    main()