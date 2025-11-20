from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q4_result import Q4IntermediateResult
import threading
from utils.heartbeat import start_heartbeat_sender

EXPECTED_EOFS = 2

class TopThreeClientsJoiner(Worker):
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, data_input_queue: MessageMiddlewareQueue, users_input_queue: MessageMiddlewareQueue,
                 output_queue: MessageMiddlewareQueue):
        self.data_input_queue = data_input_queue
        self.users_input_queue = users_input_queue
        self.output_queue = output_queue

        # estructuras compartidas entre threads
        self.users = {}
        self.users_by_store = {}
        self.eofs_by_client = {}
        self.clients = []
        self.cantidad = {}
        self.n = 0

        # locks
        self.users_lock = threading.Lock()
        self.eofs_lock = threading.Lock()

    def start(self):
        # Start Heartbeat
        self.heartbeat_sender = start_heartbeat_sender()

        t_data = threading.Thread(target=self.data_input_queue.start_consuming, args=(self.__on_message__,))
        t_stores = threading.Thread(target=self.users_input_queue.start_consuming, args=(self.__on_users_message__,))
        t_data.start()
        t_stores.start()
        t_data.join()
        t_stores.join()

    def __on_users_message__(self, message):
        logging.info("Procesando mensaje de Users")
        message = Message.deserialize(message)

        if message.type == MESSAGE_TYPE_EOF:
            with self.eofs_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
                if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                    logging.info(f"EOF USERS recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                    return
            self._process_top_3_by_request(message.request_id)
            self._send_eof(message)
            return

        items = message.process_message()
        if message.type == MESSAGE_TYPE_USERS:
            for item in items:
                with self.users_lock:
                    key = (item.get_user_id(), message.request_id)
                    self.users[key] = item.get_birthdate()

    def __on_message__(self, msg):
        message = Message.deserialize(msg)

        if message.type == MESSAGE_TYPE_EOF:
            with self.eofs_lock: 
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
                if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                    logging.info(f"EOF recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                    return
            
            self._process_top_3_by_request(message.request_id)
            self._send_eof(message)
            return

        items = message.process_message()
        self.cantidad[message.request_id] = self.cantidad.get(message.request_id, 0) + 1

        if message.type == MESSAGE_TYPE_TRANSACTIONS:
            pre_process = dict()
            store_id = None
            for item in items:
                if item.get_user():
                    key = (item.get_user(), message.request_id)
                    #self.cantidad[key] = self.cantidad.get(key, 0) + 1
                    pre_process[key] = pre_process.get(key, 0) + 1
                    if store_id is None:
                        store_id = item.get_store() 
            if (store_id, message.request_id) not in self.users_by_store:
                self.users_by_store[(store_id, message.request_id)] = dict()
                
            for user_id, count in pre_process.items():
                self.users_by_store[(store_id, message.request_id)][user_id] = self.users_by_store[(store_id, message.request_id)].get(user_id, 0) + count


    def _process_top_3_by_request(self, request_id):
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
                with self.users_lock:
                    birthdate = self.users.get(user_id)
                if birthdate:
                    chunk += Q4IntermediateResult(store, birthdate, transaction_count).serialize()
            
            logging.info(f"CHUNKKKKKKK: {chunk}")
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, 1, chunk)
            self.output_queue.send(msg.serialize())

# Filtrar los usuarios que tienen valores en los top 3
            

    def _send_eof(self, message):
        for key, inner in self.cantidad.items():
            logging.info(f"CANTIDADDDDDDDDDDDD: {key}, {inner}")

        self.output_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            self.output_queue.close()
            self.data_input_queue.close()
            self.users_input_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


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

    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    users_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_2"])

    joiner = TopThreeClientsJoiner(data_input_queue, users_input_queue, output_queue)
    joiner.start()


if __name__ == "__main__":
    main()
