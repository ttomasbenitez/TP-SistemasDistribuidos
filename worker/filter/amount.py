from worker.base import Worker 
from pkg.dedup.sliding_window_dedup_strategy import SlidingWindowDedupStrategy
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.q1_result import Q1Result
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_1_RESULT
from multiprocessing import Process
from utils.heartbeat import start_heartbeat_sender
from Middleware.connection import PikaConnection

MAX_PENDING_SIZE = 1000
AMOUNT_THRESHOLD = 75

class FilterAmountNode(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 host: str, 
                 amount_to_filter: int,
                 total_shards: int,
                 storage_dir: str):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.connection = PikaConnection(host)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.clients = []
        self.amount_to_filter = amount_to_filter
        self.dedup_strategy = SlidingWindowDedupStrategy(total_shards, storage_dir)
        
    def start(self):
       
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_data_queue()
        # self._consume_eof_final()
        self.connection.start_consuming()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        self.dedup_strategy.load_dedup_state()
        
        def __on_message__(message_body):
            try:
                message = Message.deserialize(message_body)

                if message.type == MESSAGE_TYPE_EOF:
                    self._handle_eof(message, data_output_exchange)
                    return

                self._handle_message(message, data_output_exchange)

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")

        data_input_queue.start_consuming(__on_message__, manual_ack=False)

    def _handle_eof(self, message, data_output_exchange):
        logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
        
        data_output_exchange.send(message.serialize(), str(message.type))
        self.dedup_strategy.update_state_on_eof(message)
        
        logging.info(f"action: ACK EOF")

    def _handle_message(self, message, data_output_exchange):
        if self.dedup_strategy.check_state_before_processing(message) is False:
            return
        
        # Procesar mensaje
        logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type} | msg_num: {message.msg_num}")
        self._ensure_request(message.request_id)
        self._inc_inflight(message.request_id)

        self._process_and_send_items(message, data_output_exchange)
        
        self.dedup_strategy.update_contiguous_sequence(message)

        self._dec_inflight(message.request_id)

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
        "total_shards": int(os.getenv('TOTAL_SHARDS', 3)),
        "storage_dir": os.getenv('STORAGE_DIR', './data'),
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

    filter = FilterAmountNode(config_params["input_queue"], 
                            config_params["exchange"], 
                            config_params["rabbitmq_host"],  
                            AMOUNT_THRESHOLD,
                            config_params["total_shards"],
                            config_params["storage_dir"])
    filter.start()

if __name__ == "__main__":
    main()
