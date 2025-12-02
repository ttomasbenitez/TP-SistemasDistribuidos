from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF
from multiprocessing import Process
from Middleware.connection import PikaConnection
from utils.heartbeat import start_heartbeat_sender
from pkg.dedup.sliding_window_dedup_strategy import SlidingWindowDedupStrategy

class StoreAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str,
                 eof_service_queue: str,
                 host: str,total_shards: int,
                 storage_dir: str):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.connection = PikaConnection(host)
        self.eof_service_queue = eof_service_queue
        self.dedup_strategy = SlidingWindowDedupStrategy(total_shards, storage_dir)

    def start(self):
       
        logging.info(f"Starting process")
        # p_data = Process(target=self._consume_data_queue)
        
        self.heartbeat_sender = start_heartbeat_sender()
        # p_data.start()
        # p_data.join()
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
        self.heartbeat_sender.stop()
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        eof_service_queue = MessageMiddlewareQueue(self.eof_service_queue, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_queue, eof_service_queue])
        
        self.dedup_strategy.load_dedup_state()
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_service_queue.send(message.serialize())
                    self.dedup_strategy.update_state_on_eof(message)
                    return
                
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                
                if self.dedup_strategy.check_state_before_processing(message) is False:
                    return
                items = message.process_message()
                groups = self._group_items_by_store(items)
                self._send_groups(message, groups, data_output_queue)
                self.dedup_strategy.update_contiguous_sequence(message)
            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)

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
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "total_shards": int(os.getenv('TOTAL_SHARDS', 3)),
        "storage_dir": os.getenv('STORAGE_DIR', './data'),
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
    
    aggregator = StoreAggregator(config_params["input_queue"],
                                 config_params["output_queue"],
                                 config_params["eof_service_queue"],
                                 config_params["rabbitmq_host"],
                                 config_params["total_shards"],
                                 config_params["storage_dir"])
    aggregator.start()

if __name__ == "__main__":
    main()
