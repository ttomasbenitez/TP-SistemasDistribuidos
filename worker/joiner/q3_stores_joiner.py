from worker import Worker
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_STORES, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_3_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3Result

EXPECTED_EOFS = 2

class StoresJoiner(Worker):
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, input_exchange: MessageMiddlewareExchange, out_queue: MessageMiddlewareQueue):
        super().__init__(input_exchange)
        self.out_queue = out_queue
        self.pending_transactions = list()
        self.processed_transactions = dict()  
        self.stores = {}
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
            self.send_joined_transactions(message)
            self._send_eof(message)
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                self.stores[item.get_id()] = item.get_name()

        elif message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
            for item in items:
                store_name = self.stores.get(item.get_store())
                if store_name:
                    key = (store_name, item.get_period())
                    self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                else:
                    self.pending_transactions.append(item)

    def send_joined_transactions(self, message):
        total_chunk = ''
        for (key, total_tpv) in self.processed_transactions.items():
            store_name, period = key
            q3Result = Q3Result(store_name, period, total_tpv)
            total_chunk += q3Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_3_RESULT, message.msg_num, total_chunk)
        self.out_queue.send(msg.serialize())

    def _process_pending(self):
        for item in self.pending_transactions:
            store_name = self.stores.get(item.get_store())
            if store_name:
                key = (store_name, item.get_period())
                self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                self.pending_transactions.remove(item)

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
    config_params["output_exchange_q3"] = os.getenv('EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_q3"], 
                                        {config_params["input_queue_1"]: [str(MESSAGE_TYPE_QUERY_3_RESULT), str(MESSAGE_TYPE_EOF)], 
                                        config_params["input_queue_2"]: [str(MESSAGE_TYPE_STORES), str(MESSAGE_TYPE_EOF)]})

    joiner = StoresJoiner(output_exchange, output_queue)
    joiner.start()


if __name__ == "__main__":
    main()
