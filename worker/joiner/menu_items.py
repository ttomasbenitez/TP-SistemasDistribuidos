import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_QUERY_2_RESULT
from utils.custom_logging import initialize_log
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import os
from pkg.storage.state_storage.joiner_menu_items import JoinerMenuItemsStateStorage
from pkg.message.utils import parse_int



class JoinerMenuItems(Joiner):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 menu_items_input_queue: str,
                 host: str, 
                 expected_eofs: int,
                 storage_dir: str = None):
        super().__init_client_handler__(menu_items_input_queue, host, expected_eofs, JoinerMenuItemsStateStorage(storage_dir, {
            "menu_items": {},
            "last_by_sender": {},
            "pending_results": []
        }))
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange

    def _process_items_to_join(self, message):
        items = message.process_message()
        state = self.state_storage.get_data_from_request(message.request_id)
        menu_items_state = state.setdefault("menu_items", {})
        
        if message.type == MESSAGE_TYPE_MENU_ITEMS:
            for item in items:
                menu_items_state[item.get_id()] = item.get_name()
        
        self.state_storage.data_by_request[message.request_id] = state
        logging.info(f"action: Menu Items updated | request_id: {message.request_id}")
        
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._send_pending_clients(message, data_output_exchange)
        self._send_eof(message, data_output_exchange)
    
    def _send_pending_clients(self, message, data_output_exchange):
        request_id = message.request_id
        ready_to_send = ''
        
        self.state_storage.load_state(request_id)
        state = self.state_storage.get_data_from_request(request_id)
        pending_results = state.get("pending_results", [])
        
        for item in pending_results:
            name = item.get(parse_int(item.item_data))
            if name:
                item.join_item_name(name)
                ready_to_send += item.serialize()

        # TODO: numero mensaje
        if ready_to_send:
            serialized = Message(request_id, MESSAGE_TYPE_QUERY_2_RESULT, 0, ready_to_send).serialize()
            data_output_exchange.send(serialized, str(request_id))
            
        self.state_storage.delete_state(request_id)
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        def __on_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            # dedup/ordering for data stream
            if self.is_dupped(message, stream="data"):
                return

            try:
                items = message.process_message()
                if message.type == MESSAGE_TYPE_QUERY_2_RESULT:
                    ready_to_send = ''
                    state = self.state_storage.get_data_from_request(message.request_id)
                    menu_items = state.setdefault("menu_items", {})
                    pending_results = state.setdefault("pending_results", [])
                    for item in items:
                        name = menu_items.get(parse_int(item.item_data))
                        if name:
                            item.join_item_name(name)
                            ready_to_send += item.serialize()
                        else:
                            pending_results.append(item)

                    if ready_to_send:
                        message.update_content(ready_to_send)
                        serialized = message.serialize()
                        data_output_exchange.send(serialized, str(message.request_id))
                        
                    if len(pending_results) > 0:                
                        self.state_storage.data_by_request[message.request_id] = state
            finally:
                self.state_storage.save_state(message.request_id)
                self.state_storage.cleanup_data(message.request_id)
                    
        data_input_queue.start_consuming(__on_message__)
        
    def _send_eof(self, message, data_output_exchange):
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")

                       
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
    config_params["expected_eofs"] = int(os.getenv('EXPECTED_EOFS', '3'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

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
                                 config_params["rabbitmq_host"],
                                 config_params["expected_eofs"],
                                 config_params["storage_dir"])
    joiner.start()

if __name__ == "__main__":
    main()
