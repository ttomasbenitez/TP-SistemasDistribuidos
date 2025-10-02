from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from multiprocessing import Process, Value

class StoreAggregator(Worker):

    def __init__(self, data_in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue, eof_in_queues: list[MessageMiddlewareQueue], eof_out_queues: list[MessageMiddlewareQueue]):
        super().__init__(data_in_queue)
        self.data_in_queue = data_in_queue
        self.out_queue = out_queue
        self.eof_in_queues = eof_in_queues
        self.eof_out_queues = eof_out_queues

    def start(self):
        # Creamos un proceso por cada input queue
        processes = []

        logging.info(f"Starting process")
        p = Process(target=self._consume_data_queue, args=(self.data_in_queue,))
        p.start()
        processes.append(p)

        for queue in self.eof_in_queues:
            logging.info(f"Starting process")
            p = Process(target=self._consume_eof_queue, args=(queue,))
            p.start()
            processes.append(p)

        # Esperamos que terminen
        for p in processes:
            p.join()

    def _consume_data_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self.__on_message__)

    def _consume_eof_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self._on_eof_from_queue_message)

    def _on_eof_from_queue_message(self, message):
        try:
            logging.info("Procesando mensaje de EOF queue")
            message = Message.deserialize(message)
        
            with self.leader.get_lock():
                is_leader = self.leader.value

            if is_leader:
                with self.expected_acks.get_lock():
                    if self.expected_acks.value > 0:
                       self.expected_acks.value -= 1
                    if self.expected_acks.value <= 0:
                        self._send_final_eof(message)
            else:
                with self.processing_data.get_lock(), self.eof_received.get_lock():
                    if message.type == MESSAGE_TYPE_EOF and not self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        self.__received_EOF__(message)
                        logging.info("Procesamiento finalizado")
                        return
                    elif message.type == MESSAGE_TYPE_EOF and self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        return
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
                    
    def __on_message__(self, message):
        try:
            logging.info(f"Recibo mensaje")
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                self.__received_EOF__(message)
                return
            items = message.process_message()
            logging.info(f"Proceso mensaje | request_id: {message.request_id} | type: {message.type}")
            for item in items:
                stores_dict = dict()
                store = item.get_store()
                if store not in stores_dict:
                    stores_dict[store] = item.get_final_amount()
                else:
                    stores_dict[store] += item.get_final_amount()
            if stores_dict:
                message.update_content(stores_dict)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Filtro correctamente | request_id: {message.request_id} | type: {message.type}")
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            
    def close(self):
        try:
            self.in_middleware.close()
            self.out_queue.close()
        except Exception as e:
            print(f"Error al cerrar: {type(e).__name__}: {e}")

    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue": os.getenv('OUTPUT_QUEUE_1'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    # Claves requeridas
    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def create_eofs_queues(rabbitmq_host):
    input_queues = []
    output_queues = []

    for key, queue_name in os.environ.items():
        if key.startswith("INPUT_QUEUE_"):
            if queue_name.startswith("EOF"):
                input_queues.append(MessageMiddlewareQueue(rabbitmq_host, queue_name))
        elif key.startswith("OUTPUT_QUEUE_"):
            if queue_name.startswith("EOF"):
                output_queues.append(MessageMiddlewareQueue(rabbitmq_host, queue_name))

    return input_queues, output_queues

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    eof_input_queues, eof_output_queues = create_eofs_queues(config_params["rabbitmq_host"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])

    filter = StoreAggregator(input_queue, output_queue, eof_input_queues, eof_output_queues)
    filter.start()

if __name__ == "__main__":
    main()