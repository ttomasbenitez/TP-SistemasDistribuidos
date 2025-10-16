from pkg.message.q4_result import Q4Result
from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_STORES, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_3_RESULT
from utils.custom_logging import initialize_log
import os
import threading

EXPECTED_EOFS = 2

class Q4StoresJoiner(Worker):

    def __init__(self, data_input_queue: MessageMiddlewareQueue, stores_input_queue: MessageMiddlewareQueue,
                 out_exchange: MessageMiddlewareExchange, out_queue_name: str):
        self.data_input_queue = data_input_queue
        self.stores_input_queue = stores_input_queue
        self.out_exchange = out_exchange
        self.out_queue_prefix = out_queue_name

        # estructuras compartidas entre threads
        self.stores = {}
        self.eofs_by_client = {}
        self.pending_clients = {}
        self.processed_clients = {}
        self.clients = []

        # locks
        self.stores_lock = threading.Lock()
        self.eofs_lock = threading.Lock()

    def start(self):
        t_data = threading.Thread(target=self.data_input_queue.start_consuming, args=(self.__on_message__,))
        t_stores = threading.Thread(target=self.stores_input_queue.start_consuming, args=(self.__on_stores_message__,))
        t_data.start()
        t_stores.start()
        t_data.join()
        t_stores.join()


    def __on_stores_message__(self, message):
        logging.info("Procesando mensaje de Stores")
        message = Message.deserialize(message)

        if message.type == MESSAGE_TYPE_EOF:
            with self.eofs_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            return

        items = message.process_message()
        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                key = (item.get_id(), message.request_id)
                with self.stores_lock:
                    self.stores[key] = item.get_name()

    def __on_message__(self, msg):
        message = Message.deserialize(msg)
        logging.info(f"Received message | request_id: {message.request_id} | type: {message.type}")

        if message.request_id not in self.clients:
            out_queue_name = f"{self.out_queue_prefix}_{message.request_id}"
            self.out_exchange.add_queue_to_exchange(out_queue_name, str(message.request_id))
            self.clients.append(message.request_id)


        if message.type == MESSAGE_TYPE_EOF:
            with self.eofs_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
                if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                    logging.info(f"EOF recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                    return
            try:
                self._process_pending(request_id=message.request_id)
                self.send_processed_clients(message)
                self._send_eof(message)
            except Exception as e:
                logging.exception(f"Error publicando resultados finales: {type(e).__name__}: {e}")
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
            for item in items:
                logging.info(f"Store_id: {item.store_id} | birthday: {item.birthdate} | quantity: {item.purchases_qty}")
                with self.stores_lock:
                    store_name = self.stores.get((item.get_store(), message.request_id), 0)
                if store_name:
                    self.processed_clients[message.request_id] = Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty())
                else:
                    self.pending_clients[(item.get_store(), message.request_id)] = item

    def send_processed_clients(self, message):
        total_chunk = ''

        for req_id, q4Result in self.processed_clients.items():
            if req_id == message.request_id:
                total_chunk += q4Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, total_chunk)
        self.out_exchange.send(msg.serialize(), str(message.request_id))

    def _process_pending(self, request_id):
        for (store, req_id), item in self.pending_clients.items():
            if req_id == request_id:
                store_name = self.stores.get((store, req_id))
                if store_name:
                    self.processed_clients[req_id] = Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty())
                del self.pending_clients[(store, req_id)]

    def _send_eof(self, message):
        self.out_exchange.send(message.serialize(), str(message.request_id))
        self.clients.remove(message.request_id)
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            self.out_exchange.close()
            self.data_input_queue.close()
            self.stores_input_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue_name"] = os.getenv('OUTPUT_QUEUE')
    config_params["output_exchange_q4"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue_name"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_q4"], {})
    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    stores_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_2"])

    joiner = Q4StoresJoiner(data_input_queue, stores_input_queue, output_exchange, config_params["output_queue_name"])
    joiner.start()


if __name__ == "__main__":
    main()
