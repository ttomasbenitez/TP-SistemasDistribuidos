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
from pkg.storage.state_storage.joiner_stores import JoinerStoresQ3StateStorage
class StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_exchange: str,  
                 stores_input_queue: str,
                 host: str,
                 storage_dir: str,
                 aggregator_semester_replicas: int = 2):
        
        # Expected EOFs: 1 from stores_input_queue + N from aggregator-semester (data_input_queue)
        expected_eofs = 1 + aggregator_semester_replicas
        super().__init_client_handler__(stores_input_queue, host, expected_eofs, JoinerStoresQ3StateStorage(storage_dir, {
            "stores": {},
            "last_by_sender": {},
            "pending_results": [],
            "last_eof_count": 0,
            "last_eofs_by_node": {},
        }))
        
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            
            # Load state from disk BEFORE dedup check to get correct last_by_sender
            self.state_storage.load_state(message.request_id)
            
            # dedup/ordering for data stream
            if self.is_dupped(message, stream="data"):
                return
            
            try:
                items = message.process_message()
                if message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
                    ready_to_send = ''
                    state = self.state_storage.get_state(message.request_id)
                    store_state = state.setdefault("stores", {})
                    pending_results = state.setdefault("pending_results", [])
                    logging.info(f"action: processing Q3 results | request_id: {message.request_id} | ITEMS: {len(items)} | stores_available: {len(store_state)}")
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
                        logging.info(f"action: Q3 results sent | request_id: {message.request_id} | items_count: {len(items)}")
                        
                    if len(pending_results) > 0:                
                        self.state_storage.data_by_request[message.request_id] = state
                        logging.info(f"action: pending results stored | request_id: {message.request_id} | count: {len(pending_results)}")
            except Exception as e:
                logging.error(f"action: error processing data queue | request_id: {message.request_id} | error: {str(e)}")
            finally:
                self.state_storage.save_state(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)

    def _process_items_to_join(self, message):
        try:
            items = message.process_message()
            state = self.state_storage.get_state(message.request_id)
            store_state = state.setdefault("stores", {})
            
            if message.type == MESSAGE_TYPE_STORES:
                for item in items:
                    store_state[item.get_id()] = item.get_name()
            
            self.state_storage.data_by_request[message.request_id] = state
            logging.info(f"action: Stores updated | request_id: {message.request_id} | count: {len(items)}")
        except Exception as e:
            logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
        finally:
            self.state_storage.save_state(message.request_id)
                    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._send_pending_clients(message.request_id, data_output_exchange)
        self._send_eof(message, data_output_exchange)
        
    def _send_pending_clients(self, request_id, data_output_exchange):
        ready_to_send = ''
        self.state_storage.load_state(request_id)
        state = self.state_storage.get_state(request_id)
        
        pending_results = state.get("pending_results", [])
        stores = state.get("stores", {})
        logging.info(f"action: Processing pending Q3 results | request_id: {request_id} | pending_count: {len(pending_results)}")
        
        for item in pending_results:
            store_name = stores.get(item.get_store())
            logging.info(f"action: processing pending Q3 result | request_id: {request_id} | STORE ID: {item.get_store()} | NAME: {store_name}")
            if store_name:
                q3 = Q3Result(item.get_period(), store_name, item.get_tpv())
                ready_to_send += q3.serialize()
           
        if ready_to_send:
            out = Message(request_id, MESSAGE_TYPE_QUERY_3_RESULT, 0, ready_to_send)
            data_output_exchange.send(out.serialize(), str(request_id))
            logging.info(f"action: pending results sent | request_id: {request_id} | items_count: {len(pending_results)}")
        else:
            logging.info(f"action: no pending results to send | request_id: {request_id}")
            
        self.state_storage.delete_state(request_id)

    def _send_eof(self, message, data_output_exchange):    
        message.update_content("3")
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")
        self.state_storage.delete_state(message.request_id)

def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q3"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["aggregator_semester_replicas"] = int(os.getenv('AGGREGATOR_SEMESTER_REPLICAS', '2'))
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', '/tmp/joiner_stores_q3')

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
        config_params["storage_dir"],
        config_params["aggregator_semester_replicas"])
    joiner.start()


if __name__ == "__main__":
    main()
