from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS

class FilterAmountNode(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue, amount_to_filter: int):
        super().__init__(in_queue)
        self.out_queue = out_queue
        self.amount_to_filter = amount_to_filter
        
    def __on_message__(self, message):
        try:
            message = Message.read_from_bytes(message)
            logging.info(f"Mensaje leído | request_id: {message.request_id} | type: {message.type}")
            items = message.process_message()
            new_chunk = '' 
            for item in items:
                amount = item.get_amount()
                if amount >= self.amount_to_filter:
                    new_chunk += item.serialize()
                else:
                    logging.info(f"Amount {amount} fuera del rango permitido | request_id: {message.request_id} | type: {message.type}")
            if new_chunk:
                message.update_content(new_chunk)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Envio correctamente | request_id: {message.request_id} | type: {message.type}")

        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
    
    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize(), str(message.type))
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")
            
    def close(self):
        try:
            self.in_middleware.close()
            self.out_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")

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
    
    filter = FilterAmountNode(input_queue, output_queue, 75)
    filter.start()

if __name__ == "__main__":
    main()