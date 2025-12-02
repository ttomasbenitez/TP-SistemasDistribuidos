from worker.eof.eof_service import EofService
import logging
import os
from utils.custom_logging import initialize_log
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from pkg.message.constants import MESSAGE_TYPE_EOF
from Middleware.connection import PikaConnection
import signal

class EofServiceMonth(EofService):
    
    def __init__(self, 
                 eof_input_queque: str, 
                 output_queues: list,
                 expected_acks: int,
                 host: str):
        
        self.connection = PikaConnection(host)
        self.eof_input_queque = eof_input_queque
        self.eof_output_queues = output_queues
        self.expected_acks = expected_acks
        self.acks_by_client = dict()
        
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)
    
    def send_message(self, message):
        try:
            # eof_out_middleware is now a list of middlewares
            for middleware in self.eof_output_queues:
                queue = MessageMiddlewareQueue(middleware, self.connection)
                queue.send(message.serialize())
        except Exception as e:
            logging.error(f"Error al enviar el mensaje: {type(e).__name__}: {e}")
    
    def __handle_shutdown(self, signum, frame):
        """
        Closes all worker connections and shuts down the worker.
        """ 
        try:
            self.close()
        except Exception:
            pass
        logging.info(f'action: gateway shutdown | result: success')
        
            
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
        "expected_acks": int(os.getenv('EXPECTED_ACKS')),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    # Read multiple output queues
    output_queues = []
    i = 1
    while True:
        queue = os.getenv(f'OUTPUT_QUEUE_{i}')
        if not queue:
            # Fallback for backward compatibility or if only OUTPUT_QUEUE is defined
            if i == 1:
                queue = os.getenv('OUTPUT_QUEUE')
                if queue:
                    output_queues.append(queue)
            break
        output_queues.append(queue)
        i += 1
    
    config_params["output_queues"] = output_queues

    required_keys = [
        "rabbitmq_host",
        "input_queue",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    if not config_params["output_queues"]:
        raise ValueError("Expected at least one output queue.")

    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    eof_service = EofServiceMonth(
        config_params["input_queue"],
        config_params["output_queues"],
        config_params["expected_acks"],
        config_params["rabbitmq_host"]
    )
    eof_service.start()
    
if __name__ == "__main__":
    main()
