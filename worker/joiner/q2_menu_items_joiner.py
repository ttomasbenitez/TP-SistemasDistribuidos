import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_2_RESULT
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange
from pkg.storage.state_storage.joiner_storage import JoinerQ2StateStorage
from pkg.message.utils import parse_int
from utils.joiner import initialize_config

class Q2MenuItems(Joiner):
    
    def _process_items(self, message: Message):
        try:
            items = message.process_message()
            menu_items_state = self.joiner_storage.get_state(message.request_id)
            menu_items = menu_items_state.get("items", {})
            if items:
                for item in items:
                    menu_items[item.get_id()] = item.get_name()
            
            menu_items_state["items"] = menu_items
            self.joiner_storage.data_by_request[message.request_id] = menu_items_state
                    
        except Exception as e:
                logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
                
    def _join(self, request_id, item):
        items_state = self.joiner_storage.get_state(request_id)
        menu_items = items_state.get("items", {})
        item_name = menu_items.get(parse_int(item.item_data))
        if item_name:
            item.join_item_name(item_name)
            return item.serialize()
        return None
                            
    def _process_items_to_join(self, message: Message, data_output_exchange: MessageMiddlewareExchange):
        self._process_items_to_join_by_message_type(message, data_output_exchange, MESSAGE_TYPE_QUERY_2_RESULT)
       
    def _process_pending_clients(self, message: Message, data_output_exchange: MessageMiddlewareExchange):
        self._process_pending_clients_by_message_type(message, data_output_exchange, MESSAGE_TYPE_QUERY_2_RESULT)

    def _send_message(self, data_output_exchange: MessageMiddlewareExchange, message: Message):
        data_output_exchange.send(message.serialize(), str(message.request_id))
        
def main():
    config_params = initialize_config()

    state_storage = JoinerQ2StateStorage(config_params["storage_dir"])
    
    joiner = Q2MenuItems(config_params["input_queue_1"], 
                            config_params["input_queue_2"],
                            config_params["output_middleware"],
                            config_params["storage_dir"],
                            state_storage,
                            config_params["container_name"],
                            2,
                            config_params["rabbitmq_host"],
                            config_params["expected_eofs"])
    joiner.start()

if __name__ == "__main__":
    main()