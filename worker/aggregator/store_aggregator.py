from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF

class StoreAggregator(Worker):

    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
        
    def __on_message__(self, message):
        try:
            logging.info(f"Recibo mensaje")
            message = Message.read_from_bytes(message)
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
        "input_queue": os.getenv('INPUT_QUEUE'),
        "output_queue": os.getenv('OUTPUT_QUEUE'),
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


def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])

    filter = StoreAggregator(input_queue, output_queue)
    filter.start()

if __name__ == "__main__":
    main()