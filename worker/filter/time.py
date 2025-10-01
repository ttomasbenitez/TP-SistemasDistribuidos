from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS

class FilterTimeNode(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_exchange: MessageMiddlewareExchange, time_set):
        super().__init__(in_queue)
        self.out_exchange = out_exchange
        self.time = time_set
        
    def __on_message__(self, message):
        try:
            logging.info(f"\n\nProcesando mensaje\n\n")
            message = Message.read_from_bytes(message)
            # logging.info(f"Mensaje leído | request_id: {message.request_id} | type: {message.type} | content: {message.content}")
            items = message.process_message()
            logging.info("Mensaje procesado")
            logging.info(f"Primer item: {items[0]}")
            new_chunk = '' 
            for item in items:
                item_time = item.get_time()
                # logging.info(f"Item time: {item_time}")
                time = item_time.hour
                if time > min(self.time) and time < max(self.time):
                    new_chunk += item.serialize()
                    # logging.info(f"Filtro correctamente | request_id: {message.request_id} | type: {message.type}")
                else:
                    logging.info(f"Time {time} is outside the range. Discarding message.")
            if new_chunk:
                message.update_content(new_chunk)
                serialized = message.serialize()
                self.out_exchange.send(serialized, str(message.type))
                logging.info(f"Envio correctamente | request_id: {message.request_id} | type: {message.type}")

        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            
    def close(self):
        try:
            self.in_middleware.close()
            self.out_exchange.close()
        except Exception as e:
            print(f"Error al cerrar: {type(e).__name__}: {e}")

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
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "output_exchange_filter_time": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    # Claves requeridas
    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_exchange_filter_time",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_filter_time"], 
                                        {config_params["output_queue_1"]: str(MESSAGE_TYPE_TRANSACTIONS), 
                                        config_params["output_queue_2"]: str(MESSAGE_TYPE_TRANSACTIONS)})
    
    filter = FilterTimeNode(input_queue, output_exchange, {6, 23})
    filter.start()

if __name__ == "__main__":
    main()