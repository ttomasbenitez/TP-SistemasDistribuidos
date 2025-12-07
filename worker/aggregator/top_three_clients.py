from worker.base import Worker
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q4_result import Q4IntermediateResult
from Middleware.connection import PikaConnection
from pkg.dedup.dedup_by_sender_strategy import DedupBySenderStrategy
from pkg.storage.state_storage.eof_storage import EofStorage
from pkg.storage.state_storage.top_three_clients_storage import TopThreeClientsStateStorage
from utils.heartbeat import start_heartbeat_sender

SNAPSHOT_INTERVAL = 2000

class TopThreeClients(Worker):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str, 
                 storage_dir: str,
                 host: str,
                 expected_eofs: int,
                 container_name: str):
        
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.connection = PikaConnection(host)
        self.state_storage = TopThreeClientsStateStorage(storage_dir)
        self.dedup_strategy = DedupBySenderStrategy(self.state_storage)
        self.eof_storage = EofStorage(storage_dir)
        self.expected_eofs = expected_eofs
        self.node_id = container_name
        self.snapshot_interval = {}
        
    def start(self):
        logging.info(f"action: startup | loading persisted state for recovery")
        
        self.state_storage.load_state_all()
        self.eof_storage.load_state_all()
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                if self.on_eof_message(message, self.dedup_strategy, self.eof_storage, self.expected_eofs):
                    logging.info(f"action: all_eofs_received | request_id: {message.request_id} | sending results")
                    self._send_results(message, data_output_queue)
                    self.eof_storage.delete_state(message.request_id)
                    self.dedup_strategy.clean_dedup_state(message.request_id)
                return
                
            if self.dedup_strategy.is_duplicate(message):
                logging.info(f"action: duplicated_message | request_id: {message.request_id}")
                return
                
            items = message.process_message()
            if items:
                self._accumulate_transactions(items, message.request_id)
         
        data_input_queue.start_consuming(__on_message__)
        
    def _accumulate_transactions(self, items, request_id):
        """Acumula transacciones por tienda y usuario (solo en memoria)."""
            
        state = self.state_storage.get_state(request_id)
        users_by_store = state["users_by_store"]
        
        for item in items:
            user_id = item.get_user()
            if not user_id:
                continue

            store_id = item.get_store()
            if store_id is None:
                continue
                
            store_users = users_by_store.setdefault(store_id, {})
            store_users[user_id] = store_users.get(user_id, 0) + 1
        
        state["users_by_store"] = users_by_store
         # Contador por request_id para decidir cuándo hacer snapshot
        # self.snapshot_interval.setdefault(request_id, 0)
        # self.snapshot_interval[request_id] += 1
        
        # if self.snapshot_interval[request_id] >= SNAPSHOT_INTERVAL:
        #     self.snapshot_interval[request_id] = 0
        #     self.state_storage.save_state(request_id)
        # else:
        #     self.state_storage.append_state(request_id)
   
    def _send_results(self, message: Message, data_output_queue: MessageMiddlewareQueue):
        
        """Send 1 message per store with top-3 users joined with birthdates."""
        current_state = self.state_storage.get_state(message.request_id)
        
        users_by_store_state = current_state.get("users_by_store", {})
        
        new_msg_num = 0

        for store_id, users in users_by_store_state.items():
            
            if store_id is None:
                continue

            sorted_users = sorted(users.items(), key=lambda x: (-x[1], x[0]))

            unique_values = []
            for user_id, count in sorted_users:
                if count not in unique_values:
                    unique_values.append(count)
                if len(unique_values) == 3:
                    break

            top_3_users = [user for user in sorted_users if user[1] in unique_values]

            chunk = ""
            for user_id, transaction_count in top_3_users:
                res = Q4IntermediateResult(store_id, user_id, transaction_count)
                chunk += res.serialize()
            
            if chunk:
                new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, new_msg_num, chunk, self.node_id)
                data_output_queue.send(new_message.serialize())
                new_msg_num += 1
                logging.info(f"action: store_result_sent | request_id: {message.request_id} | store_id: {store_id} | top_3_count: {len(top_3_users)}")
        
        new_eof = Message(message.request_id, MESSAGE_TYPE_EOF, new_msg_num, '', self.node_id)
        new_msg_num += 1
        data_output_queue.send(new_eof.serialize())
        logging.info(f"action: eof_forwarded | request_id: {message.request_id} | node_id: {self.node_id}")
        
def initialize_config():
    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["expected_eofs"] = int(os.getenv('EXPECTED_EOFS'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')
    config_params["container_name"] = os.getenv('CONTAINER_NAME')

    if None in [config_params["rabbitmq_host"], config_params["input_queue"],
                config_params["output_queue"], config_params["expected_eofs"], 
                config_params["container_name"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params

def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    top_3 = TopThreeClients(
        config_params["input_queue"], 
        config_params["output_queue"], 
        config_params["storage_dir"], 
        config_params["rabbitmq_host"], 
        config_params["expected_eofs"],
        config_params["container_name"]
    )
    top_3.start()


if __name__ == "__main__":
    main()