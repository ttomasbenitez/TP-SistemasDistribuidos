from Middleware.middleware import MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from pkg.message.constants import MESSAGE_TYPE_QUERY_3_RESULT
from pkg.message.q3_result import Q3Result
from utils.joiner import initialize_config
from pkg.storage.state_storage.joiner_storage import JoinerQ3StateStorage
class Q3Stores(Joiner):
   
    def _process_items_to_join(self, message: Message): 
        data_output_exchange = MessageMiddlewareExchange(self.data_output_middleware, {}, self.connection)
        
        try:
            stores = message.process_message() 
            ready_to_send = ''
            request_id = message.request_id
        
            state = self.state_storage.get_state(request_id)
            pending_results = state.get("pending_results", [])
            current_msg_num = state.get("current_msg_num", -1)
            new_pending = []
            
            for item in pending_results:
                store = next((store for store in stores if item.get_store() == store.get_id()), None)
                
                if store:
                    store_name = store.get_name()
                    q3 = Q3Result(item.get_period(), store_name, item.get_tpv())
                    ready_to_send += q3.serialize()
                    
                else: 
                    new_pending.append(item)
            
            if ready_to_send:
                new_msg_num = current_msg_num + 1
                out = Message(request_id, MESSAGE_TYPE_QUERY_3_RESULT, new_msg_num, ready_to_send)
                data_output_exchange.send(out.serialize(), str(request_id))
                logging.info(f"action: pending results sent | request_id: {request_id} | items_count: {len(pending_results)}")
                state["current_msg_num"] = new_msg_num
            else:
                logging.info(f"action: no pending results to send | request_id: {request_id}")
            
            state["pending_results"] = new_pending
            
        except Exception as e:
            logging.error(f"action: process_items_error | request_id: {message.request_id} | error: {type(e).__name__}: {e}")
        
        logging.info(f"action: Processing pending Q3 results | request_id: {request_id} | pending_count: {len(pending_results)}")

def main():
    config_params = initialize_config()

    state_storage = JoinerQ3StateStorage(config_params["storage_dir"])
    
    joiner = Q3Stores(config_params["input_queue_1"], 
                            config_params["input_queue_2"],
                            config_params["output_middleware"],
                            state_storage,
                            config_params["container_name"],
                            3,
                            config_params["rabbitmq_host"],
                            config_params["expected_eofs"])
    joiner.start()


if __name__ == "__main__":
    main()
