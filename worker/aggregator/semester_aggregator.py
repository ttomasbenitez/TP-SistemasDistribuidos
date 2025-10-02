from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT

class SemesterAggregator(Worker):

    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
        self.suma_test = 0
        self.log = False

    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"Suma acumulada 2024-H1 tienda 1: {self.suma_test}")
                self.__received_EOF__(message)
                return
            items = message.process_message()
            agg = dict()
            store_id = None

            for it in items:
               
                year = it.get_year()
                sem  = it.get_semester()
                period = f"{year}-H{sem}"
                if store_id is None:
                    store_id = it.store_id
                amount = it.get_final_amount()
                agg[period] = agg.get(period, 0.0) + amount
            self.log = True
            for period, total in agg.items():
                res = Q3IntermediateResult(period, store_id, total)
                self._send_grouped_item(message, res)
            
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            
    def _send_grouped_item(self, message, item):
        if item == None:
            return
        if item.store_id == 1 and item.year_half_created_at == "2024-H1":
            self.suma_test += item.intermediate_tpv
        new_chunk = item.serialize()
        new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, message.msg_num, new_chunk)
        logging.info(f"Enviando mensaje | request_id: {new_message.request_id} | type: {new_message.type}")
        self.out_queue.send(new_message.serialize())
            
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
    
    filter = SemesterAggregator(input_queue, output_queue)
    filter.start()

if __name__ == "__main__":
    main()