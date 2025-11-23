import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_QUERY_2_RESULT
from utils.custom_logging import initialize_log
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import os
from pkg.message.utils import parse_int

EXPECTED_EOFS = 2

class JoinerMenuItems(Joiner):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 menu_items_input_queue: str,
                 host: str):
        super().__init_client_handler__(menu_items_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.pending_items = []

    def _process_items_to_join(self, message):
        items = message.process_message()
        
        if message.type == MESSAGE_TYPE_MENU_ITEMS:
            for item in items:
                with self.items_to_join_lock:
                    if message.request_id not in self.items_to_join:
                        self.items_to_join[message.request_id] = {}
                    self.items_to_join[message.request_id][item.get_id()] = item.get_name()
        
        logging.debug(f"action: Menu Items updated | request_id: {message.request_id}")
    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        self.message_middlewares.append(data_output_exchange)
        
        request_id = message.request_id
        ready_to_send = ''
        
        for (item, req_id) in list(self.pending_items):
            if req_id != request_id:
                continue
            name = self.items_to_join.get(parse_int(item.item_data))
            if name:
                item.join_item_name(name)
                ready_to_send += item.serialize()
                self.pending_items.remove((item, req_id))

        if ready_to_send:
            serialized = Message(request_id, MESSAGE_TYPE_QUERY_2_RESULT, 0, ready_to_send).serialize()
            data_output_exchange.send(serialized, str(request_id))

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        def __on_message__(message):
            message = Message.deserialize(message)
            logging.debug(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            items = message.process_message()

            if message.type == MESSAGE_TYPE_QUERY_2_RESULT:
                ready_to_send = ''
                for item in items:
                    with self.items_to_join_lock:
                        name = self.items_to_join.get(message.request_id, {}).get(parse_int(item.item_data))
                    if name:
                        item.join_item_name(name)
                        ready_to_send += item.serialize()
                    else:
                        self.pending_items.append((item, message.request_id))

                if ready_to_send:
                    message.update_content(ready_to_send)
                    serialized = message.serialize()
                    data_output_exchange.send(serialized, str(message.request_id))
                    
        data_input_queue.start_consuming(__on_message__)
                       
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
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q2"] = os.getenv('OUTPUT_EXCHANGE')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in (config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_exchange_q2"]):
        raise ValueError("Expected value not found. Aborting.")
    
    return config_params

def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = JoinerMenuItems(config_params["input_queue_1"],
                                 config_params["output_exchange_q2"],
                                 config_params["input_queue_2"],
                                 config_params["rabbitmq_host"])
    joiner.start()

if __name__ == "__main__":
    main()
