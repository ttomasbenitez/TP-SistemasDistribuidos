from worker.base import Worker
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import (
    MESSAGE_TYPE_EOF,
    MESSAGE_TYPE_STORES,
    MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_3_RESULT,
)
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3Result
import threading

EXPECTED_EOFS = 2


class StoresJoiner(Worker):

    def __init__(self, data_input_queue: str, stores_input_queue: str,
                 host: str, out_exchange: str, out_queue_name: str):
        self.data_input_queue = data_input_queue
        self.stores_input_queue = stores_input_queue
        self.host = host
        self.out_exchange = out_exchange
        self.out_queue_prefix = out_queue_name

        # estructuras compartidas entre threads
        self.stores = {}
        self.eofs_by_client = {}
        self.pending_transactions = []
        self.processed_transactions = {}
        self.clients = []

        # locks
        self.stores_lock = threading.Lock()
        self.eofs_lock = threading.Lock()

    def start(self):
        t_data = threading.Thread(target=self._consume_data_queue)
        t_stores = threading.Thread(target=self._consume_stores_queue)
        t_data.start()
        t_stores.start()
        t_data.join()
        t_stores.join()
        
    def _consume_data_queue(self):
        out_exchange = MessageMiddlewareExchange(self.host, self.out_exchange, {})
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"Received message | request_id: {message.request_id} | type: {message.type}")

            if message.request_id not in self.clients:
                out_queue_name = f"{self.out_queue_prefix}_{message.request_id}"
                out_exchange.add_queue_to_exchange(out_queue_name, str(message.request_id))
                self.clients.append(message.request_id)

            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"EOF recibido | request_id: {message.request_id}")
                with self.eofs_lock:
                    self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
                    if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                        return

                try:
                    self._process_pending()
                    self.send_joined_transactions_by_request(message, message.request_id, out_exchange)
                    self._send_eof(message, out_exchange)
                except Exception as e:
                    logging.exception(f"Error publicando resultados finales: {type(e).__name__}: {e}")
                return

            items = message.process_message()

            if message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
                for item in items:
                    with self.stores_lock:
                        store_name = self.stores.get(message.request_id, {}).get(item.get_store())
                    if store_name:
                        key = (message.request_id, store_name, item.get_period())
                        self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                    else:
                        self.pending_transactions.append((item, message.request_id))
                        
        data_input_queue.start_consuming(__on_message__)

    def _consume_stores_queue(self):
        stores_input_queue = MessageMiddlewareQueue(self.host, self.stores_input_queue)
        stores_input_queue.start_consuming(self.__on_stores_message__)
    
    def __on_stores_message__(self, message):
        message = Message.deserialize(message)

        if message.type == MESSAGE_TYPE_EOF:
            with self.eofs_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            return

        items = message.process_message()
        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                with self.stores_lock:
                    if message.request_id not in self.stores:
                        self.stores[message.request_id] = {}
                    self.stores[message.request_id][item.get_id()] = item.get_name()

 
    def _process_pending(self):
        for item, request_id in list(self.pending_transactions):
            with self.stores_lock:
                store_name = self.stores.get(request_id, {}).get(item.get_store())
            if store_name:
                key = (request_id, store_name, item.get_period())
                self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                self.pending_transactions.remove((item, request_id))

    def send_joined_transactions_by_request(self, message, request_id, out_exchange):
        total_chunk = ''
        for (key, total_tpv) in self.processed_transactions.items():
            req_id, store_name, period = key
            if req_id != request_id:
                continue
            q3Result = Q3Result(period, store_name, total_tpv)
            total_chunk += q3Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_3_RESULT, message.msg_num, total_chunk)
        out_exchange.send(msg.serialize(), str(message.request_id))
        logging.info(f"Resultados enviados | request_id: {request_id}")

    def _send_eof(self, message, out_exchange):
        out_exchange.send(message.serialize(), str(message.request_id))
        self.clients.remove(message.request_id)
        logging.info(f"EOF enviado | request_id: {message.request_id}")

    def close(self):
        try:
            # self.out_exchange.close()
            pass
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["input_exchange_q3"] = os.getenv('INPUT_EXCHANGE_NAME')
    config_params["output_queue_name"] = os.getenv('OUTPUT_QUEUE')
    config_params["output_exchange_q3"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue_name"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = StoresJoiner(config_params["input_queue_1"],  config_params["input_queue_2"], config_params["rabbitmq_host"], config_params["output_exchange_q3"], config_params["output_queue_name"])
    joiner.start()


if __name__ == "__main__":
    main()
