from pkg.message.q4_result import Q4Result
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_STORES
from utils.custom_logging import initialize_log
import os
from pkg.storage.state_storage.joiner_stores import JoinerStoresQ4StateStorage

CANTIDAD_REPLICAS_AGGREGATOR_STORE = 2
CANTIDAD_CLIENTES = 2

class Q4StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str, 
                 stores_input_queue: str,
                 host: str,
                 storage_dir: str,
                 top_three_clients_replicas: int = 3):
        # Expected EOFs: 1 from stores + N from top_three_clients replicas

        expected_eofs = 1 + top_three_clients_replicas
        
        super().__init_client_handler__(stores_input_queue, host, expected_eofs, JoinerStoresQ4StateStorage(storage_dir, {
            "stores": {},
            "last_by_sender": {},
            "pending_results": [],
            "last_eof_count": 0,
        }))
        
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
               
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self.state_storage.load_state(message.request_id)
        self._send_pending_clients(message, data_output_exchange)
        self._send_eof(message, data_output_exchange)
        
    def _process_items_to_join(self, message):
        try:
            items = message.process_message()
            state = self.state_storage.get_state(message.request_id)
            store_state = state.setdefault("stores", {})
            if message.type == MESSAGE_TYPE_STORES:
                for item in items:
                    store_state[item.get_id()] = item.get_name()
            
            self.state_storage.data_by_request[message.request_id] = state
            logging.info(f"action: Stores updated | request_id: {message.request_id}")
        except Exception as e:
            logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue,  self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received from top_three_clients | request_id: {message.request_id} | type: {message.type}")
            
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            
            if self.is_dupped(message, stream="data"):
                return
            try:
                items = message.process_message() 
                chunk = ''
                if message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
                    state = self.state_storage.get_state(message.request_id)
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
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)


    def _send_pending_clients(self, message, data_output_exchange):
        self.state_storage.load_state(message.request_id)
        state = self.state_storage.get_state(message.request_id)
        
        pending_results = state.get("pending_results", [])
        logging.info(f"action: Processing pending Q4 results | request_id: {message.request_id} | pending_count: {len(pending_results)}")
        stores = state.get("stores", {})
        chunk = ''
        for item in pending_results:
            store_name = stores.get(item.get_store(), None)
            chunk += Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()).serialize()
        logging.info(f"action: Pending Q4 results processed | request_id: {message.request_id} | chunk_size: {chunk}")
        if chunk:
            msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, chunk)
            data_output_exchange.send(msg.serialize(), str(message.request_id))
        
    def _send_eof(self, message, data_output_exchange):
        message.update_content("4")
        data_output_exchange.send(message.serialize(), str(message.request_id))
        last_eof_count += 1
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")
        self.state_storage.delete_state(message.request_id)


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q4"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', '/tmp/joiner_q4_storage')
    config_params["top_three_clients_replicas"] = int(os.getenv('TOP_THREE_CLIENTS_REPLICAS', '3'))

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
                            config_params["storage_dir"],
                            config_params["top_three_clients_replicas"])
    joiner.start()

if __name__ == "__main__":
    main()
