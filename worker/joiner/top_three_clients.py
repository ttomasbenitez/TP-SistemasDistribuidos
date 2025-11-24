from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q4_result import Q4IntermediateResult

EXPECTED_EOFS = 2

class TopThreeClientsJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 users_input_queue: str,
                 host: str):
        
        super().__init_client_handler__(users_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue

        self.users_by_store = {}

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            items = message.process_message()

            if message.type == MESSAGE_TYPE_TRANSACTIONS:
                pre_process = dict()
                store_id = None
                for item in items:
                    if item.get_user():
                        key = (item.get_user(), message.request_id)
                        pre_process[key] = pre_process.get(key, 0) + 1
                        if store_id is None:
                            store_id = item.get_store() 
                if (store_id, message.request_id) not in self.users_by_store:
                    self.users_by_store[(store_id, message.request_id)] = dict()
                    
                for user_id, count in pre_process.items():
                    self.users_by_store[(store_id, message.request_id)][user_id] = self.users_by_store[(store_id, message.request_id)].get(user_id, 0) + count

        data_input_queue.start_consuming(__on_message__)
        
    def _process_items_to_join(self, message):
        items = message.process_message()
        if message.type == MESSAGE_TYPE_USERS:
            for item in items:
                with self.items_to_join_lock:
                    key = (item.get_user_id(), message.request_id)
                    self.items_to_join[key] = item.get_birthdate()
        
    def _send_results(self, message):
        data_output_queue = MessageMiddlewareQueue(self.host, self.data_output_queue)
        self.message_middlewares.append(data_output_queue)
        self._process_top_3_by_request(message.request_id, data_output_queue)
        self._send_eof(message, data_output_queue)
        
    def _process_top_3_by_request(self, request_id, data_output_queue):
        for (store, req_id), users in self.users_by_store.items():
            if store is None:
                continue
            
            if req_id != request_id:
                continue

            unique_values = []

            sorted_users = sorted(users.items(), key=lambda x: (-x[1], x[0]))
 
            for user in sorted_users:
                if user[1] not in unique_values:
                    unique_values.append(user[1])
                if len(unique_values) == 3:
                    break

            top_3_users = [user for user in sorted_users if user[1] in unique_values]
            chunk = ''
            for user in top_3_users:
                user_id, transaction_count = user
                with self.items_to_join_lock:
                    birthdate = self.items_to_join.get(user_id)
                if birthdate:
                    chunk += Q4IntermediateResult(store, birthdate, transaction_count).serialize()
            
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, 1, chunk)
            data_output_queue.send(msg.serialize())
        
        self.users_by_store = {k: v for k, v in self.users_by_store.items() if k[1] != request_id}           

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
                                   config_params["rabbitmq_host"])
    joiner.start()


if __name__ == "__main__":
    main()
