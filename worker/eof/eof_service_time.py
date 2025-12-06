from worker.eof.eof_service import EofService
import logging
import os
from utils.custom_logging import initialize_log
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from pkg.message.constants import MESSAGE_TYPE_EOF

class EofServiceTime(EofService):
    
    def __init__(self, eof_input_queque, eof_output_middleware, eof_queues, expected_eofs, host):
        super().__init__(eof_input_queque, eof_output_middleware, expected_eofs, host)
        self.eof_queues = eof_queues
    
    def send_message(self, message):
        eof_output_exchange = MessageMiddlewareExchange(
            self.eof_output_middleware,
            self.eof_queues,
            self.connection
        )
        try:
            routing_key = str(message.type)
            eof_output_exchange.send(message.serialize(), routing_key)
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
        "output_exchange_filter": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "expected_eofs": int(os.getenv('EXPECTED_ACKS')),
    }
    
    output_queues = [
        value
        for key, value in os.environ.items()
        if key.startswith("OUTPUT_QUEUE")
    ]
    
    config_params["output_queues"] = output_queues

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_exchange_filter",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    if len(output_queues) == 0:
         raise ValueError("Expected at least one output queues. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
 
    eof_queues = {}
    for queue in config_params["output_queues"]:
        eof_queues[queue] = [str(MESSAGE_TYPE_EOF)]

    eof_service = EofServiceTime(
        config_params["input_queue"],
        config_params["output_exchange_filter"],
        eof_queues,
        config_params["expected_eofs"],
        config_params["rabbitmq_host"]
    )
    eof_service.start()
    
if __name__ == "__main__":
    main()
