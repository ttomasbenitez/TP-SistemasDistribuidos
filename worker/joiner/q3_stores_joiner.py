from worker import Worker
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_STORES, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_3_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3Result
import threading

EXPECTED_EOFS = 2

class StoresJoiner(Worker):
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, input_queue: MessageMiddlewareQueue, out_exchange: MessageMiddlewareExchange, out_queue_name: str, in_queue_names: list[str], rabbitmq_host: str):
        #super().__init__(input_exchange)
        self.in_queue = input_queue
        self.out_exchange = out_exchange
        self.out_queue_prefix = out_queue_name
        self.pending_transactions = list()
        self.processed_transactions = dict()  
        self.stores_lock = threading.Lock()
        self.stores = {}
        self.eofs_by_client_lock = threading.Lock()
        self.eofs_by_client = {}
        self.clients = []
        self.in_queues_names = in_queue_names
        self.stores_threads = []
        self.rabbitmq_host = rabbitmq_host
        self.now = 0

    def start(self):
        thread = threading.Thread(target=self.in_queue.start_consuming, args=(self.__on_message__,))
        thread.start()

    def __on_stores_message__(self, message):
        logging.info("Procesando mensaje de Stores")

        message = Message.deserialize(message)
        if message.type == MESSAGE_TYPE_EOF:
            logging.info(f"EOF recibido | request_id: {message.request_id} | type: {message.type}")
            with self.eofs_by_client_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            return
        
        stores = message.process_message()
        if message.type == MESSAGE_TYPE_STORES:
            for store in stores:
                with self.stores_lock:
                    if message.request_id not in self.stores:
                        self.stores[message.request_id] = {}
                    self.stores[message.request_id][store.get_id()] = store.get_name()

    def __on_message__(self, msg):
        message = Message.deserialize(msg)
        #logging.info(f"Received message | request_id: {message.request_id} | type: {message.type}")
        logging.info(f"{self.now}")
        self.now += 1
        if message.request_id not in self.clients:
            logging.error("New client")
            if self.in_queues_names:
                in_queue_name = f"{self.in_queues_names.pop()}"
                logging.error(f"IN QUEUE NAME {in_queue_name}")
                queue = MessageMiddlewareQueue(self.rabbitmq_host, in_queue_name)
                logging.error(f"CREADA QUEUE")
                thread = threading.Thread(target=queue.start_consuming, args=(self.__on_stores_message__,))
                self.stores_threads.append(thread)
                thread.daemon = True
                thread.start()
                logging.error(f"EMPEZADO THREAD")
            else:
                logging.error("No quedan colas disponibles")

            out_queue_name = f"{self.out_queue_prefix}_{message.request_id}"
            self.out_exchange.add_queue_to_exchange(out_queue_name, str(message.request_id))
            self.clients.append(message.request_id)

        if message.type == MESSAGE_TYPE_EOF:
            logging.info(f"EOF recibido | request_id: {message.request_id} | type: {message.type}")
            with self.eofs_by_client_lock:
                self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
                if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                    logging.info(f"EOF recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS}")
                    return

            # --- ÚLTIMO EOF: procesar y publicar usando out_mw (channel separado) ---
            try:
                self._process_pending()                                               
                self.send_joined_transactions_by_request(message, message.request_id)  
                self._send_eof(message)                                                 
            except Exception as e:
                logging.exception(f"Error publicando resultados finales: {type(e).__name__}: {e}")
            return

        # --- Mensajes de datos ---
        items = message.process_message()

        if message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
            for item in items:
                with self.stores_lock:
                    store_name = self.stores.get(message.request_id, {}).get(item.get_store())
                if store_name:
                    key = (message.request_id, store_name, item.get_period())
                    self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                else:
                    item_to_append = (item, message.request_id)
                    self.pending_transactions.append(item_to_append)

    def send_joined_transactions_by_request(self, message, request_id):
        logging.info("ENVIANDOOOOOOOOOO")
        total_chunk = ''
        for (key, total_tpv) in self.processed_transactions.items():
            req_id, store_name, period = key
            if request_id != req_id:
                continue
            q3Result = Q3Result(period, store_name, total_tpv)
            total_chunk += q3Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_3_RESULT, message.msg_num, total_chunk)
        self.out_exchange.send(msg.serialize(), str(message.request_id))
        logging.info("ENVIADOOOOOOOOOO")

    def _process_pending(self):
        for item, request_id in self.pending_transactions:
            with self.stores_lock:
                store_name = self.stores.get(request_id, {}).get(item.get_store())
            if store_name:
                key = (request_id, store_name, item.get_period())
                self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                self.pending_transactions.remove((item, request_id))

    def _send_eof(self, message):
        self.out_exchange.send(message.serialize(), str(message.request_id))
        self.clients.remove(message.request_id)
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            for thread in self.stores_threads:
                thread.join()
            for _, queue in self.queues:
                queue.close()
            self.out_exchange.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")

def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["input_queue_3"] = os.getenv('INPUT_QUEUE_3')
    config_params["input_exchange_q3"] = os.getenv('INPUT_EXCHANGE_NAME')
    config_params["output_queue_name"] = os.getenv('OUTPUT_QUEUE')
    config_params["output_exchange_q3"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["input_queue_3"], 
                config_params["output_queue_name"],]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_q3"], {})

    #input_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["input_exchange_q3"], 
    #                                    {config_params["input_queue_1"]: [str(MESSAGE_TYPE_QUERY_3_RESULT), str(MESSAGE_TYPE_EOF)]})
    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    in_queues_names = [config_params["input_queue_2"], config_params["input_queue_3"]]

    joiner = StoresJoiner(input_queue, output_exchange, config_params["output_queue_name"], in_queues_names, config_params["rabbitmq_host"])
    joiner.start()


if __name__ == "__main__":
    main()

