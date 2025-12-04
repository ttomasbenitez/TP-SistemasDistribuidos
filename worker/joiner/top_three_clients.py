from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.storage.state_storage.top_three_clients import TopThreeClientsStateStorage
from pkg.message.q4_result import Q4IntermediateResult
import threading

EXPECTED_EOFS = 3 # 1 users, 2 store agg
SNAPSHOT_COUNT = 1000

class TopThreeClientsJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 users_input_queue: str,
                 host: str,
                 storage_dir: str,
                 container_name: str = None):
        
        super().__init_client_handler__(users_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.state_storage = TopThreeClientsStateStorage(storage_dir)
        # Sequencing / dedup state
        self._sender_lock = threading.Lock()
        self._last_msg_by_sender_data = {}
        self._last_msg_by_sender_users = {}
        
        # Message numbering based on replica ID
        try:
            replica_id = int(container_name.split('-')[-1]) if container_name else 1
        except (ValueError, AttributeError, IndexError):
            replica_id = 1
            logging.error(f"Could not parse replica_id from {container_name}, defaulting to 1")
        
        self.msg_num_counter = replica_id * 1000000

    def start(self):
        # Load persisted state once on startup and hydrate last-msg maps
        self.state_storage.load_state_all()
        for _rid, st in self.state_storage.data_by_request.items():
            for sid, num in st.get("last_msg_by_sender_data", {}).items():
                self._last_msg_by_sender_data[sid] = max(self._last_msg_by_sender_data.get(sid, -1), num)
            for sid, num in st.get("last_msg_by_sender_users", {}).items():
                self._last_msg_by_sender_users[sid] = max(self._last_msg_by_sender_users.get(sid, -1), num)
        super().start()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            try:
                # Dedup/ordering for data stream (transactions)
                sender_id = message.get_node_id_and_request_id()
                with self._sender_lock:
                    last = self._last_msg_by_sender_data.get(sender_id, -1)
                    if message.msg_num <= last:
                        if message.msg_num == last:
                            logging.info(f"action: duplicate_msg | stream:data | sender:{sender_id} | msg:{message.msg_num}")
                        else:
                            logging.warning(f"action: out_of_order_msg | stream:data | sender:{sender_id} | msg:{message.msg_num} < last:{last}")
                        return
                    self._last_msg_by_sender_data[sender_id] = message.msg_num
                items = message.process_message()
                if message.type == MESSAGE_TYPE_TRANSACTIONS:
                    self._accumulate_items(items, message.request_id, sender_id, message.msg_num)
            finally:
                if message.type == MESSAGE_TYPE_TRANSACTIONS:
                    self._dec_inflight(message.request_id)
                
        data_input_queue.start_consuming(__on_message__)
        
    def _accumulate_items(self, items, request_id, sender_id=None, msg_num=None):
        with self.state_storage._lock:
            state = self.state_storage.data_by_request.setdefault(request_id, {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender_data": {},
                "last_msg_by_sender_users": {},
            })
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
            # persist last seen marker for data stream if provided
            if sender_id is not None and msg_num is not None:
                state["last_msg_by_sender_data"][sender_id] = msg_num
        self.state_storage.save_state(request_id)
        logging.info(f"Snapshot guardado | request_id: {request_id}")
        
    def _process_items_to_join(self, message):
        with self.state_storage._lock:
            # Dedup/ordering for users stream
            sender_id = message.get_node_id_and_request_id()
            with self._sender_lock:
                last = self._last_msg_by_sender_users.get(sender_id, -1)
                if message.msg_num <= last:
                    if message.msg_num == last:
                        logging.info(f"action: duplicate_msg | stream:users | sender:{sender_id} | msg:{message.msg_num}")
                        return
                    else:
                        logging.warning(f"action: out_of_order_msg | stream:users | sender:{sender_id} | msg:{message.msg_num} < last:{last}")
                        return
                self._last_msg_by_sender_users[sender_id] = message.msg_num

            items = message.process_message()
            state = self.state_storage.data_by_request.setdefault(message.request_id, {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender_data": {},
                "last_msg_by_sender_users": {},
            })
            users_birthdates = state["users_birthdates"]

            for item in items:
                user_id = item.get_user_id()
                birthdate = item.get_birthdate()
                users_birthdates[user_id] = birthdate
            state["last_msg_by_sender_users"][sender_id] = message.msg_num

        self.state_storage.save_state(message.request_id)
        
    def _send_results(self, message):
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
        
        if chunk:
            new_msg_num = self.msg_num_counter
            self.msg_num_counter += 1
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, new_msg_num, chunk)
            data_output_queue.send(msg.serialize())
            logging.info(f"Results sent | request_id: {request_id} | msg_num: {new_msg_num}")
        
        
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
    config_params["container_name"] = os.getenv('CONTAINER_NAME', 'top-three-clients')

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
                                   config_params["storage_dir"],
                                   config_params["container_name"])
    joiner.start()


if __name__ == "__main__":
    main()
