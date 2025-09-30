from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os

class FilterYearNode(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue, out_exchange: MessageMiddlewareExchange, years_set):
        super().__init__(in_queue)
        self.out_queue = out_queue
        self.out_exchange = out_exchange
        self.years = years_set
        
    def __on_message__(self, message):
        try:
            logging.info("Procesando mensaje")
            message = Message.__deserialize__(message)
            items = message.process_message()
            logging.info("Mensaje procesado")
            new_chunk = b''
            for item in items:
                year = item.get_year()
                if year in self.years:
                    new_chunk += item.serialize()
            if new_chunk:
                new_message = message.update_content(new_chunk)
                serialized = new_message.serialize()
                #self.out_queue.send(serialized)
                #self.out_exchange.send(serialized)
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            
    def close(self):
        try:
            self.in_middleware.close()
            self.out_queue.close()
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

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    
    filter = FilterYearNode(input_queue, None, None, {2024, 2025})
    filter.start()

if __name__ == "__main__":
    main()