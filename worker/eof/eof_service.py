from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF
import os
from utils.custom_logging import initialize_log, setup_process_logger


class EofService(Worker):
  
    def __init__(self, expected_acks: int, eof_in_queque: MessageMiddlewareQueue, eof_out_exchange: MessageMiddlewareExchange):
        super().__init__(eof_in_queque)
        self.eof_out_exchange = eof_out_exchange
        self.expected_acks = expected_acks
        self.acks_by_client = dict()
      
    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                self.acks_by_client[message.request_id] = self.acks_by_client.get(message.request_id, 0) + 1  
                if self.acks_by_client[message.request_id] == self.expected_acks:
                    logging.info(f"Enviando final EOF del cliente {message.request_id}")
                    self.eof_out_exchange.send(message.serialize(), str(message.type))
                    del self.acks_by_client[message.request_id]
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
    
    def send_message(self, message):
        try:
            self.in_middleware.send(message)
        except Exception as e:
            print(f"Error al enviar el mensaje: {type(e).__name__}: {e}")
              
            
    def close(self):
        try:
            self.in_middleware.close()
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
        "input_queue": os.getenv('INPUT_QUEUE'),
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "output_queue_3": os.getenv('OUTPUT_QUEUE_3'),
        "output_exchange_filter_year": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "expected_acks": int(os.getenv('EXPECTED_ACKS')),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_queue_3",
        "output_exchange_filter_year",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    eof_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    eof_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_filter_year"], 
                                        {config_params["output_queue_1"]: [str(MESSAGE_TYPE_EOF)], 
                                        config_params["output_queue_2"]: [str(MESSAGE_TYPE_EOF)], 
                                        config_params["output_queue_3"]: [str(MESSAGE_TYPE_EOF)]})

    eof_service = EofService(
        expected_acks=config_params["expected_acks"],
        eof_in_queque=eof_input_queue,
        eof_out_exchange=eof_output_exchange
    )
    eof_service.start()
if __name__ == "__main__":
    main()
