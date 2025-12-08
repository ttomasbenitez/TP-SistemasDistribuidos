from pkg.message.q4_result import Q4Result
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_4_RESULT
from utils.joiner import initialize_config
from pkg.storage.state_storage.joiner import JoinerQ4StateStorage
from pkg.message.utils import parse_int

class Q4Stores(Joiner):
    
    def _process_items(self, message: Message):
        try:
            items = message.process_message()
            stores_state = self.joiner_storage.get_state(message.request_id)
            stores = stores_state.get("items", {})
            if items:
                for item in items:
                    stores[item.get_id()] = item.get_name()
            
            stores_state["items"] = stores
            self.joiner_storage.data_by_request[message.request_id] = stores_state
                    
        except Exception as e:
                logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
        
    def _join(self, request_id, item):
        items_state = self.joiner_storage.get_state(request_id)
        stores = items_state.get("items", {})
        store_name = stores.get(item.get_store())
        if store_name:
            q4 = Q4Result(store_name, item.get_user_id(), item.get_purchases_qty())
            return q4.serialize()
        return None    
    
    def _process_items_to_join(self, message: Message, data_output_exchange: MessageMiddlewareExchange):
        self._process_items_to_join_by_message_type(message, data_output_exchange, MESSAGE_TYPE_QUERY_4_RESULT)
       
    def _process_pending_clients(self, message: Message, data_output_exchange: MessageMiddlewareExchange):
        self._process_pending_clients_by_message_type(message, data_output_exchange, MESSAGE_TYPE_QUERY_4_RESULT)
        
    def _send_message(self, data_output_exchange: MessageMiddlewareExchange, message: Message):
        data_output_exchange.send(message.serialize(), str(message.request_id))           
   
def main():
    config_params = initialize_config()
    
    state_storage = JoinerQ4StateStorage(config_params["storage_dir"])
    
    joiner = Q4Stores(config_params["input_queue_1"], 
                        config_params["input_queue_2"],
                        config_params["output_middleware"],
                        config_params["storage_dir"],
                        state_storage,
                        config_params["container_name"],
                        4,
                        config_params["rabbitmq_host"],
                        config_params["expected_eofs"])
    joiner.start()

if __name__ == "__main__":
    main()
