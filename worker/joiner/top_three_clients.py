from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.storage.state_storage.top_three_clients import TopThreeClientsStateStorage
from pkg.message.q4_result import Q4IntermediateResult

EXPECTED_EOFS = 3 # 1 users, 2 store agg
SNAPSHOT_COUNT = 1000

class TopThreeClientsJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 users_input_queue: str,
                 host: str,
                 storage_dir: str):
        
        super().__init_client_handler__(users_input_queue, host, EXPECTED_EOFS, TopThreeClientsStateStorage(storage_dir, {
            "users_by_store": {},
            "users_birthdates": {},
            "last_by_sender": {}
        }))
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            
            if self.is_dupped(message, stream="data"):
                return
            try:
                items = message.process_message()
                if message.type == MESSAGE_TYPE_TRANSACTIONS:
                    self._accumulate_items(items, message.request_id)
            finally:
                self.state_storage.save_state(message.request_id)
                self.state_storage.cleanup_data(message.request_id)
                
        data_input_queue.start_consuming(__on_message__)
        
    def _accumulate_items(self, items, request_id):
        state = self.state_storage.get_data_from_request(request_id)
        users_by_store = state["users_by_store"]
        store_id = None

        for item in items:
            user_id = item.get_user()
            if not user_id:
                continue

            store_id = store_id or item.get_store()
            store_users = users_by_store.setdefault(store_id, {})
            store_users[user_id] = store_users.get(user_id, 0) + 1

        if store_id is None:
            return
            
        state["users_by_store"] = users_by_store
        self.state_storage.data_by_request[request_id] = state
        
    def _process_items_to_join(self, message):
        items = message.process_message()
        state = self.state_storage.get_data_from_request(message.request_id)
        users_birthdates = state["users_birthdates"]

        for item in items:
            user_id = item.get_user_id()
            birthdate = item.get_birthdate()
            users_birthdates[user_id] = birthdate
    
        self.state_storage.data_by_request[message.request_id] = state
        
    def _send_results(self, message):
        self.state_storage.load_state(message.request_id)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        self.message_middlewares.append(data_output_queue)
        self._process_top_3_by_request(message.request_id, data_output_queue)
        self._send_eof(message, data_output_queue)
        self.state_storage.delete_state(message.request_id)
        
    def _process_top_3_by_request(self, request_id, data_output_queue):
        saved_state = self.state_storage.data_by_request.get(request_id, {})
        users_by_store_state = saved_state.get("users_by_store", {})
        users_birthdates_state = saved_state.get("users_birthdates", {})
        
        chunk = ''

        for store, users in users_by_store_state.items():
            if store is None:
                continue

            # users: dict user_id -> count
            sorted_users = sorted(users.items(), key=lambda x: (-x[1], x[0]))

            unique_values = []
            for user_id, count in sorted_users:
                if count not in unique_values:
                    unique_values.append(count)
                if len(unique_values) == 3:
                    break

            top_3_users = [user for user in sorted_users if user[1] in unique_values]

            for user_id, transaction_count in top_3_users:
                birthdate = users_birthdates_state.get(user_id)
                if birthdate:
                    chunk += Q4IntermediateResult(store, birthdate, transaction_count).serialize()
        
        # TODO: numero de mensaje
        if chunk:
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, 1, chunk)
            data_output_queue.send(msg.serialize())
        
        
    def _send_eof(self, message, data_output_queue):
        data_output_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = TopThreeClientsJoiner(config_params["input_queue_1"], 
                                   config_params["output_queue"], 
                                   config_params["input_queue_2"], 
                                   config_params["rabbitmq_host"],
                                   config_params["storage_dir"])
    joiner.start()


if __name__ == "__main__":
    main()
