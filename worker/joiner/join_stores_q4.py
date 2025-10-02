from pkg.message.q4_result import Q4Result
from worker import Worker
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
        self.pending_clients = list()
        self.processed_clients = list()
        self.stores = dict()
        self.eof_count = 0

    def __on_message__(self, msg):
        message = Message.deserialize(msg)
        logging.info(f"Received message | request_id: {message.request_id} | type: {message.type}")
        if message.type == MESSAGE_TYPE_EOF:
            self.eof_count += 1
            if self.eof_count < EXPECTED_EOFS:
                logging.info(f"EOF recibido {self.eof_count}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                return
            self._process_pending()
            self.send_processed_clients(message)
            self._send_eof(message)
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                self.stores[item.get_id()] = item.get_name()

        elif message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
            for item in items:
                store_name = self.stores.get(item.get_store(), 0)
                if store_name:
                    self.processed_clients.append(Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()))
                else:
                    self.pending_clients.append(item)

    def send_processed_clients(self, message):
        total_chunk = ''
        for q4Result in self.processed_clients:
            total_chunk += q4Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, total_chunk)
        self.out_queue.send(msg.serialize())

    def _process_pending(self):
        for item in self.pending_clients:
            store_name = self.stores.get(item.get_store())
            if store_name:
                self.processed_clients.append((store_name, item.get_birthdate(), item.get_purchases_qty()))
                self.pending_clients.remove(item)

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
