from pkg.message.q4_result import Q4Result
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_4_RESULT
from utils.joiner import initialize_config
from pkg.storage.state_storage.joiner_storage import JoinerQ4StateStorage
from pkg.message.utils import parse_int

class Q4Stores(Joiner):
            
   def _process_items_to_join(self, message: Message): 

        data_output_exchange = MessageMiddlewareExchange(self.data_output_middleware,{},self.connection)
    
        stores = message.process_message()
        request_id = message.request_id

        if not stores:
            logging.info(f"action: no_stores_to_join | request_id: {request_id}")
            return
            
        state = self.state_storage.get_state(request_id)
        pending_results = state.get("pending_results", [])
        current_msg_num = state.get("current_msg_num", -1)

        if not pending_results:
            logging.info(f"action: no_pending_results | request_id: {request_id}")
            return

        stores_by_id = {}
        for store in stores:
            stores_by_id[store.get_id()] = store
                
        chunk = ''
        new_pending = []

        logging.info(f"length of stores results: {len(stores)}")
        
        for item in pending_results:
                
            item_store_id = parse_int(item.get_store())
            logging.info(f"item_store_id: {item_store_id}")
                
            store = stores_by_id.get(item_store_id)
            logging.info(f"matched store: {store}")
            if not store:
                new_pending.append(item)
                continue

            store_name = store.get_name()
            q4 = Q4Result(store_name, item.get_user_id(), item.get_purchases_qty()).serialize()
            chunk += q4

        if chunk:
            new_msg_num = current_msg_num + 1
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_RESULT, new_msg_num, chunk, self.node_id)
            data_output_exchange.send(msg.serialize(), str(request_id)) 
            state["current_msg_num"] = new_msg_num
        else:
            logging.info(
                f"action: join_no_matches | request_id: {request_id} "
                f"| total_pending: {len(pending_results)}"
            )
        state["pending_results"] = new_pending
        
def main():
    config_params = initialize_config()
    
    state_storage = JoinerQ4StateStorage(config_params["storage_dir"])
    
    joiner = Q4Stores(config_params["input_queue_1"], 
                        config_params["input_queue_2"],
                        config_params["output_middleware"],
                        state_storage,
                        config_params["container_name"],
                        4,
                        config_params["rabbitmq_host"],
                        config_params["expected_eofs"])
    joiner.start()

if __name__ == "__main__":
    main()
