from pkg.message.q4_result import Q4Result
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_STORES
from utils.custom_logging import initialize_log
from pkg.storage.state_storage.joiner_stores import JoinerStoresQ4StateStorage

import os

EXPECTED_EOFS = 3

class Q4StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str, 
                 stores_input_queue: str,
                 host: str,
                 storage_dir: str):
        super().__init_client_handler__(stores_input_queue, host, EXPECTED_EOFS, JoinerStoresQ4StateStorage(storage_dir, {
            "stores": {},
            "last_by_sender": {},
            "pending_results": []
        }))
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        
    def _process_items_to_join(self, message):
        items = message.process_message()
        state = self.state_storage.get_data_from_request(message.request_id)
        store_state = state.setdefault("stores", {})
        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                store_state[item.get_id()] = item.get_name()
        
        self.state_storage.data_by_request[message.request_id] = state
        logging.info(f"action: Stores updated | request_id: {message.request_id}")
                    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._send_pending_clients(message, data_output_exchange)
        self._send_eof(message, data_output_exchange)
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue,  self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append([data_input_queue, data_output_exchange])
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")
            
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            if self.is_dupped(message, stream="data"):
                return
            
            try:
                items = message.process_message() 
                chunk = ''
                if message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
                    state = self.state_storage.get_data_from_request(message.request_id)
                    store_state = state.setdefault("stores", {})
                    pending_results = state.setdefault("pending_results", [])
                    for item in items:
                        store_name = store_state.get(item.get_store(), None)
                        if store_name:
                            chunk += Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()).serialize()
                        else:
                            pending_results.append(item)
                if chunk:
                    msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, chunk)
                    data_output_exchange.send(msg.serialize(), str(message.request_id))
                if len(pending_results) > 0:
                    self.state_storage.data_by_request[message.request_id] = state
            finally:
                self.state_storage.save_state(message.request_id)
                self.state_storage.cleanup_data(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)
    
    def _send_pending_clients(self, message, data_output_exchange):
        self.state_storage.load_state(message.request_id)
        state = self.state_storage.get_data_from_request(message.request_id)
        
        pending_results = state.get("pending_results", [])
        stores = state.get("stores", {})
        chunk = ''
        for item in pending_results:
            store_name = stores.get(item.get_store(), None)
            chunk += Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()).serialize()
            
        if chunk:
            msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, chunk)
            data_output_exchange.send(msg.serialize(), str(message.request_id))
        
    def _send_eof(self, message, data_output_exchange):
        message.update_content("4")
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")
        self.state_storage.delete_state(message.request_id)

def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q4"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    
    joiner = Q4StoresJoiner(config_params["input_queue_1"], 
                            config_params["output_exchange_q4"],
                            config_params["input_queue_2"],
                            config_params["rabbitmq_host"],
                            config_params["storage_dir"])
    joiner.start()

if __name__ == "__main__":
    main()
