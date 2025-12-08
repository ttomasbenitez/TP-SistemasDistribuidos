from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_EOF
from pkg.storage.state_storage.joiner_storage import JoinerQ4StateStorage
from pkg.message.q4_result import Q4IntermediateResult
from utils.joiner import initialize_config
from pkg.message.utils import parse_int
from utils.heartbeat import start_heartbeat_sender

class Q4Users(Joiner):
    
    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
        self.state_storage.load_state_all()
        self.joiner_storage.load_state_all()
        self.connection.start()
        data_output_middleware = MessageMiddlewareQueue(self.data_output_middleware, self.connection)
        self.consume_items_to_join_queue(data_output_middleware)
        self.consume_data_queue(data_output_middleware)
        
        self.connection.start_consuming()
        
    def _process_items(self, message: Message):
        try:
            items = message.process_message()
            users_state = self.joiner_storage.get_state(message.request_id)
            users = users_state.get("items", {})
            
            if items:
                for item in items:
                    users[item.get_id()] = item.get_birthdate()
            
            users_state["items"] = users
            self.joiner_storage.data_by_request[message.request_id] = users_state
                    
        except Exception as e:
                logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
        
    def _join(self, request_id, item):
        items_state = self.joiner_storage.get_state(request_id)
        users = items_state.get("items", {})
        birthdate = users.get(parse_int(item.get_user_id()))
        logging.info(f"action: joining item | request_id: {request_id} | user_id: {item.get_user_id()} | birthdate: {birthdate}")
        if birthdate:
            q4 = Q4IntermediateResult(item.get_store(), birthdate, item.get_purchases_qty())
            return q4.serialize()
        return None
    
    def _process_items_to_join(self, message: Message, data_output_queue: MessageMiddlewareQueue):
        self._process_items_to_join_by_message_type(message, data_output_queue, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT)
       
    def _process_pending_clients(self, message: Message, data_output_queue: MessageMiddlewareQueue):
        self._process_pending_clients_by_message_type(message, data_output_queue, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT)
        
    def _send_message(self, data_output_middleware: MessageMiddlewareQueue, message: Message):
        data_output_middleware.send(message.serialize())

def main():
    config_params = initialize_config()

    state_storage = JoinerQ4StateStorage(config_params["storage_dir"])
    
    joiner = Q4Users(config_params["input_queue_1"], 
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