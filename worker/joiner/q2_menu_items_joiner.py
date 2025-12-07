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
                            
    def _process_items_to_join(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_middleware, {}, self.connection)
      
        try:
            items = message.process_message()  
            state = self.state_storage.get_state(message.request_id)
            pending_results = state.get("pending_results", [])
            current_msg_num = state.get("current_msg_num", -1)
            new_pending = []
            ready_to_send = ''
            
            for item in pending_results:
                menu_item = next((menu_item for menu_item in items if parse_int(item.item_data) == menu_item.get_id()), None)
                if menu_item:
                    name = menu_item.get_name()
                    item.join_item_name(name)
                    ready_to_send += item.serialize()
                    
                else:
                    new_pending.append(item)
                    
            if ready_to_send:
                new_msg_num = current_msg_num + 1
                msg = Message(message.request_id, MESSAGE_TYPE_QUERY_2_RESULT, new_msg_num, ready_to_send)
                data_output_exchange.send(msg.serialize(), str(message.request_id))
                logging.info(f"action: Pending Q2 results sent after join | request_id: {message.request_id} | items_count: {len(pending_results)}")
                state["current_msg_num"] = new_msg_num
            
            state["pending_results"] = new_pending
           
        except Exception as e:
            logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")

def main():
    config_params = initialize_config()

    state_storage = JoinerQ2StateStorage(config_params["storage_dir"])
    
    joiner = Q2MenuItems(config_params["input_queue_1"], 
                            config_params["input_queue_2"],
                            config_params["output_middleware"],
                            state_storage,
                            config_params["container_name"],
                            2,
                            config_params["rabbitmq_host"],
                            config_params["expected_eofs"])
    joiner.start()

if __name__ == "__main__":
    main()