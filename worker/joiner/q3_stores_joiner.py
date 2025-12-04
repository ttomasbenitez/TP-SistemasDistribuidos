from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from pkg.message.constants import (
    MESSAGE_TYPE_EOF,
    MESSAGE_TYPE_STORES,
    MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_3_RESULT,
)
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3Result
import hashlib
import threading
from pkg.storage.state_storage.joiner_stores import JoinerStoresQ3StateStorage

EXPECTED_EOFS = 3 # 1 stores, 2 semester

class StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_exchange: str,  
                 stores_input_queue: str,
                 host: str, 
                 storage_dir: str):
        
        super().__init_client_handler__(stores_input_queue, host, EXPECTED_EOFS, JoinerStoresQ3StateStorage(storage_dir, {
            "stores": {},
            "last_by_sender": {},
            "pending_results": []
        }))
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.pending_transactions = []
        self._seen_lock = threading.Lock()
        self._seen_messages = set()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            # dedup/ordering for data stream
            if self.is_dupped(message, stream="data"):
                return
           
            try:
                # Idempotencia adicional por hash del cuerpo (opc.)
                # try:
                #     raw_bytes = msg if isinstance(msg, (bytes, bytearray)) else str(msg).encode("utf-8")
                # except Exception:
                #     raw_bytes = message.serialize().encode("utf-8")
                # body_hash = hashlib.sha256(raw_bytes).hexdigest()
                # dedup_key = (message.request_id, message.type, body_hash)
                # with self._seen_lock:
                #     if dedup_key in self._seen_messages:
                #         return
                #     self._seen_messages.add(dedup_key)

                items = message.process_message()
                
                if message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
                    ready_to_send = ''
                    state = self.state_storage.get_data_from_request(message.request_id)
                    store_state = state.setdefault("stores", {})
                    pending_results = state.setdefault("pending_results", [])
                    for item in items:
                        store_name = store_state.get(item.get_store())
                        if store_name:
                            q3 = Q3Result(item.get_period(), store_name, item.get_tpv())
                            ready_to_send += q3.serialize()
                        else:
                            pending_results.append(item)
                            logging.info(f"action: Q3Result pending store join | request_id: {message.request_id} | store_id: {item.get_store()}")
                    if ready_to_send:
                        out = Message(message.request_id, MESSAGE_TYPE_QUERY_3_RESULT, message.msg_num, ready_to_send)
                        data_output_exchange.send(out.serialize(), str(message.request_id))
                    if len(pending_results):
                        self.state_storage.data_by_request[message.request_id] = state            
            finally:
                self.state_storage.save_state(message.request_id)
                self.state_storage.cleanup_data(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)

    def _process_items_to_join(self, message):
        try:
            items = message.process_message()
            state = self.state_storage.get_data_from_request(message.request_id)
            store_state = state.setdefault("stores", {})
            if message.type == MESSAGE_TYPE_STORES:
                for item in items:
                    store_state[item.get_id()] = item.get_name()
        
            self.state_storage.data_by_request[message.request_id] = state
            logging.info(f"action: Stores updated | request_id: {message.request_id}")
        except Exception as e:
            logging.error(f"Error processing items to join: {e}")
                    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._send_pending_clients(message.request_id, data_output_exchange)
        self._send_eof(message, data_output_exchange)
 
    def _send_pending_clients(self, request_id, data_output_exchange):
        ready_to_send = ''
        self.state_storage.load_state(request_id)
        state = self.state_storage.get_data_from_request(request_id)
        
        pending_results = state.get("pending_results", [])
        stores = state.get("stores", {})
        logging.info(f"action: Processing pending Q3 results | request_id: {request_id} | pending_count: {len(pending_results)} | {pending_results}")
        for item in pending_results:
            store_name = stores.get(item.get_store())
            if store_name:
                q3 = Q3Result(item.get_period(), store_name, item.get_tpv())
                ready_to_send += q3.serialize()
           
        if ready_to_send:
            out = Message(request_id, MESSAGE_TYPE_QUERY_3_RESULT, 0, ready_to_send)
            data_output_exchange.send(out.serialize(), str(request_id))
            
        self.state_storage.delete_state(request_id)

    def _send_eof(self, message, data_output_exchange):
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"EOF sent | request_id: {message.request_id}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q3"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = StoresJoiner(
        config_params["input_queue_1"],
        config_params["output_exchange_q3"],
        config_params["input_queue_2"], 
        config_params["rabbitmq_host"],
        config_params["storage_dir"])
    joiner.start()


if __name__ == "__main__":
    main()
