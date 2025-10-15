from pkg.message.q4_result import Q4Result
from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_STORES, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_3_RESULT
from utils.custom_logging import initialize_log
import os

EXPECTED_EOFS = 2

class Q4StoresJoiner(Worker):
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, input_queues: MessageMiddlewareExchange, out_queue: MessageMiddlewareQueue):
        super().__init__(input_queues)
        self.out_queue = out_queue
        self.pending_clients = dict()
        self.processed_clients = dict()
        self.stores = dict()
        self.eofs_by_client = {}

    def __on_message__(self, msg):
        message = Message.deserialize(msg)
        logging.info(f"Received message | request_id: {message.request_id} | type: {message.type}")
        if message.type == MESSAGE_TYPE_EOF:
            self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                logging.info(f"EOF recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                return
            self._process_pending(request_id=message.request_id)
            self.send_processed_clients(message)
            self._send_eof(message)
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                key = (item.get_id(), message.request_id)
                self.stores[key] = item.get_name()

        elif message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
            for item in items:
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
        self.out_queue.send(msg.serialize())

    def _process_pending(self, request_id):
        for (store, req_id), item in self.pending_clients.items():
            if req_id == request_id:
                store_name = self.stores.get((store, req_id))
                if store_name:
                    self.processed_clients[req_id] = Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty())
                del self.pending_clients[(store, req_id)]

    def _send_eof(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            for _, queue in self.queues:
                queue.close()
            self.out_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["exchange"] = os.getenv('EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    input_queues = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["exchange"], 
                                             {config_params["input_queue_1"]: [str(MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT), str(MESSAGE_TYPE_EOF)], 
                                              config_params["input_queue_2"]: [str(MESSAGE_TYPE_STORES), str(MESSAGE_TYPE_EOF)]})

    joiner = Q4StoresJoiner(input_queues, output_queue)
    joiner.start()


if __name__ == "__main__":
    main()
