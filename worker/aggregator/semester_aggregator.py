from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS

class SemesterAggregator(Worker):

    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue

    def __on_message__(self, message):
        try:
            logging.info(f"Recibo mensaje")
            message = Message.read_from_bytes(message)
            items = message.process_message()
            logging.info(f"Proceso mensaje | request_id: {message.request_id} | type: {message.type}")
            for item in items:
                year = item.get_year()
                amount = item.get_final_amount()
                semester = item.get_semester()
                message_to_send = Message(message.request_id, message.type, message.msg_num, f"{item.transaction_id};{item.store_id};{item.user_id};{amount};{item.created_at}\n")
                if year == 2024 and semester == 1:
                    new_chunk_1 += message_to_send.serialize()
                elif year == 2024 and semester == 2:
                    new_chunk_2 += message_to_send.serialize()
                elif year == 2025 and semester == 1:
                    new_chunk_3 += message_to_send.serialize()
                elif year == 2025 and semester == 2:
                    new_chunk_4 += message_to_send.serialize()
            if new_chunk_1:
                message.update_content(new_chunk_1)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Filtro correctamente | request_id: {message.request_id} | type: {message.type}")
            if new_chunk_2:
                message.update_content(new_chunk_2)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Filtro correctamente | request_id: {message.request_id} | type: {message.type}")
            if new_chunk_3:
                message.update_content(new_chunk_3)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Filtro correctamente | request_id: {message.request_id} | type: {message.type}")
            if new_chunk_4:
                message.update_content(new_chunk_4)
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
    
    filter = SemesterAggregator(input_queue, output_queue)
    filter.start()

if __name__ == "__main__":
    main()