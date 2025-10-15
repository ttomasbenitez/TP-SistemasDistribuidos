from worker.eof.eof_service import EofService
import logging
import os
from utils.custom_logging import initialize_log
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from pkg.message.constants import MESSAGE_TYPE_EOF

class EofServiceYear(EofService):
    
    def send_message_to_output(self, message):
        try:
            routing_key = str(message.type)
            self.eof_out_middleware.send(message.serialize(), routing_key)
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
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "output_queue_3": os.getenv('OUTPUT_QUEUE_3', ''), # Ver cómo hacemos con este caso
        "output_exchange_filter": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "expected_acks": int(os.getenv('EXPECTED_ACKS')),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_exchange_filter",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    eof_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    
    eof_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_filter"], 
                                            {config_params["output_queue_1"]: [str(MESSAGE_TYPE_EOF)], 
                                            config_params["output_queue_2"]: [str(MESSAGE_TYPE_EOF)], 
                                            config_params["output_queue_3"]: [str(MESSAGE_TYPE_EOF)]})

    eof_service = EofServiceYear(
        expected_acks=config_params["expected_acks"],
        eof_in_queque=eof_input_queue,
        eof_out_middleware=eof_output_exchange
    )
    eof_service.start()
    
if __name__ == "__main__":
    main()
