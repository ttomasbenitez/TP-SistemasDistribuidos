from worker.eof.eof_service import EofService
import logging
import os
from utils.custom_logging import initialize_log
from Middleware.middleware import MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF

class EofServiceAggregatorStore(EofService):
    
    def send_message(self, message):
        eof_output_queue = MessageMiddlewareQueue(
            self.eof_output_middleware,
            self.connection
        )
        try:
            eof_output_queue.send(message.serialize())
        except Exception as e:
            logging.error(f"Error al enviar el mensaje: {type(e).__name__}: {e}")
            
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
        "expected_acks": int(os.getenv('EXPECTED_ACKS')),
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

    eof_service = EofServiceAggregatorStore(
        config_params["input_queue"],
        config_params["output_queue"],
        config_params["expected_acks"],
        config_params["rabbitmq_host"]
    )
    eof_service.start()
    
if __name__ == "__main__":
    main()
