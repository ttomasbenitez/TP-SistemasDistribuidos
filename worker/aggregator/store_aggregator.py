from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF
from multiprocessing import Process, Value
from utils.heartbeat import start_heartbeat_sender


class StoreAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str,
                 eof_output_exchange: str, 
                 eof_output_queues: str,
                 eof_self_queue: str,
                 eof_service_queue: str,
                 host: str):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.host = host
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue

    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_queue = MessageMiddlewareQueue(self.host, self.data_output_queue)
        eof_output_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_input_queue, data_output_queue, eof_output_exchange])
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_output_exchange.send(message.serialize(), str(message.type))
                    return
                
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id) 
                
                items = message.process_message()
                groups = self._group_items_by_store(items)
                self._send_groups(message, groups, data_output_queue)
            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                self._dec_inflight(message.request_id)
        
        data_input_queue.start_consuming(__on_message__, prefetch_count=1)

    def _group_items_by_store(self, items):
        groups = {}
        for item in items:
            store = item.get_store()
            groups.setdefault(store, []).append(item)
        return groups
               
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
    
    eof_output_queues = {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                    config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    
    aggregator = StoreAggregator(config_params["input_queue"],
                                 config_params["output_queue"], 
                                 config_params["eof_exchange_name"], 
                                 eof_output_queues,
                                 config_params["eof_self_queue"],
                                 config_params["eof_service_queue"],
                                 config_params["rabbitmq_host"])
    aggregator.start()

if __name__ == "__main__":
    main()
