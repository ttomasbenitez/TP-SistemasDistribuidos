from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_EOF
from pkg.storage.state_storage.joiner_storage import JoinerQ4StateStorage
from pkg.message.q4_result import Q4IntermediateResult
from utils.joiner import initialize_config
from pkg.message.utils import parse_int

class Q4Users(Joiner):
                
    def _process_items_to_join(self, message: Message): 
        
        data_output_queue = MessageMiddlewareQueue(self.data_output_middleware, self.connection)
        
        users = message.process_message()
        request_id = message.request_id

        if not users:
            logging.info(f"action: no_users_to_join | request_id: {request_id}")
            return
            
        state = self.state_storage.get_state(request_id)
        pending_results = state.get("pending_results", [])
        current_msg_num = state.get("current_msg_num", -1)
        if not pending_results:
            logging.info(f"action: no_pending_results | request_id: {request_id}")
            return

        users_by_id = {}
        new_pending = []
        chunk = ''
        
        for u in users:
            users_by_id[u.get_id()] = u
            
        for item in pending_results:
            item_user_id = parse_int(item.get_user_id())
            user = users_by_id.get(item_user_id)
            if not user:
                new_pending.append(item)
                continue
            
            user_birthdate = user.get_birthdate()
            q4 = Q4IntermediateResult(item.get_store(), user_birthdate, item.get_purchases_qty())
            chunk += q4.serialize()

        if chunk:
            new_msg_num = current_msg_num + 1
            msg = Message(
                request_id,
                MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT,
                new_msg_num,
                chunk,
                self.node_id
            )
            data_output_queue.send(msg.serialize())
            state["current_msg_num"] = new_msg_num
        else:
            logging.info(
                f"action: join_no_matches | request_id: {request_id} "
                f"| pending_results_total: {len(pending_results)}"
            )
        
        state["pending_results"] = new_pending
            
    def _send_eof(self, message: Message):
        data_output_queue = MessageMiddlewareQueue(self.data_output_middleware, self.connection)
        current_state = self.state_storage.get_state(message.request_id)
        current_msg_num = current_state.get("current_msg_num", -1)
        new_msg_num = current_msg_num + 1
        new_eof = Message(message.request_id, MESSAGE_TYPE_EOF, new_msg_num, self.query_num, self.node_id)
        self.state_storage.delete_state(message.request_id)
        data_output_queue.send(new_eof.serialize())
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")

        
def main():
    config_params = initialize_config()

    state_storage = JoinerQ4StateStorage(config_params["storage_dir"])
    
    joiner = Q4Users(config_params["input_queue_1"], 
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