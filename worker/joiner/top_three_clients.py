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
SNAPSHOT_INTERVAL = 100  # Guardar snapshot cada N mensajes

class TopThreeClientsJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 users_input_queue: str,
                 host: str,
                 storage_dir: str,
                 node_id: str = None):
        
        super().__init_client_handler__(users_input_queue, host, EXPECTED_EOFS, TopThreeClientsStateStorage(storage_dir, {
            "users_by_store": {},
            "users_birthdates": {},
            "last_by_sender": {},
            "last_eof_count": 0,
            "msg_num_counter": 0,
        }))
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.node_id = node_id
     

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            # Solo procesar TRANSACTIONS aquí
            # Los USERS llegan por el otro queue (_process_items_to_join)
            if message.type != MESSAGE_TYPE_TRANSACTIONS:
                return
            
            # Dedup/ordering check
            if self.is_dupped(message, stream="data"):
                return
                
            items = message.process_message()
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
        
        self.state_storage.save_state(request_id)
        logging.info(f"action: snapshot_saved | request_id: {request_id} | type: transactions")
        
        logging.debug(f"action: transactions_accumulated | request_id: {request_id} | items: {len(items)}")

    def _process_items_to_join(self, message):
        """Process user items from the users queue (abstract method implementation)."""
        
        items = message.process_message()
        # Acumular usuarios directamente sin pasar por _consume_data_queue
        
        state = self.state_storage.get_state(message.request_id)
        users_birthdates = state["users_birthdates"]

        for item in items:
            user_id = item.get_user_id()
            birthdate = item.get_birthdate()
            if user_id and birthdate:
                users_birthdates[user_id] = birthdate

        self.state_storage.save_state(message.request_id)
        logging.info(f"action: snapshot_saved | request_id: {message.request_id}")
        
        logging.debug(f"action: users_accumulated | request_id: {message.request_id} | items: {len(items)}")
        
    def _send_results(self, message):
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        self.message_middlewares.append(data_output_queue)
        
        self._process_top_3_by_request(message.request_id, data_output_queue)
        self._send_eof(message, data_output_queue)
        self.state_storage.delete_state(message.request_id)
        
    def _process_top_3_by_request(self, request_id, data_output_queue):
        """Send 1 message per store with top-3 users joined with birthdates."""
        self.state_storage.load_state(request_id)
        current_state = self.state_storage.get_state(request_id)
        
        users_by_store_state = current_state.get("users_by_store", {})
        users_birthdates_state = current_state.get("users_birthdates", {})
        
        stores_sent = 0

        for store_id, users in users_by_store_state.items():
            if store_id is None:
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

            # Construir un SOLO chunk/mensaje para este store con todos sus top-3 usuarios
            chunk = ""
            for user_id, transaction_count in top_3_users:
                birthdate = users_birthdates_state.get(user_id)
                if birthdate:
                    res = Q4IntermediateResult(store_id, birthdate, transaction_count)
                    chunk += res.serialize()
            
            # Enviar UN SOLO mensaje por store con todos sus top-3 usuarios
            if chunk:
                new_msg_num = self.msg_num_counter
                self.msg_num_counter += 1
                new_message = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, new_msg_num, chunk, self.node_id)
                data_output_queue.send(new_message.serialize())
                stores_sent += 1
                logging.debug(f"action: store_result_sent | request_id: {request_id} | store_id: {store_id} | top_3_count: {len(top_3_users)}")
        
        logging.info(f"action: send_results_done | request_id: {request_id} | stores_sent: {stores_sent}")
        
    def _send_eof(self, message, data_output_queue):
        """Forward EOF downstream - ensure only 1 EOF per request_id."""
        self.eofs_sent.add(message.request_id)
        data_output_queue.send(message.serialize())
        logging.info(f"action: eof_forwarded | request_id: {message.request_id} | node_id: {self.node_id}")
        self.state_storage.delete_state(message.request_id)


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')
    config_params["node_id"] = os.getenv('NODE_ID', '1')

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
                                   config_params["node_id"])
    joiner.start()


if __name__ == "__main__":
    main()