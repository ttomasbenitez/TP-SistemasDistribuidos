from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log, setup_process_logger
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_TRANSACTIONS
from multiprocessing import Process, Value

class FilterTimeNode(Worker):
    
    def __init__(self, 
                 data_in_queue: MessageMiddlewareQueue, 
                 data_out_exchange: MessageMiddlewareExchange, 
                 eof_out_exchange: MessageMiddlewareExchange,
                 host: str, eof_service_queue: str, eof_self_queue: str,
                 time_set):
        self.__init_manager__()
        self.data_input_queue = data_in_queue
        self.data_out_exchange = data_out_exchange
        self.eof_out_exchange = eof_out_exchange
        self.host = host
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue
        self.time = time_set


    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue, args=(self.data_input_queue,))
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        # Esperamos que terminen
        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()
        

    def _consume_data_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self.__on_message__)

    def _consume_eof(self): 
        eof_service_queue = MessageMiddlewareQueue(self.host, self.eof_service_queue)
        eof_self_queue = MessageMiddlewareQueue(self.host, self.eof_self_queue)
    
        def on_eof_message(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en nodo | request_id: {message.request_id} | type: {message.type}")
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    eof_service_queue.send(message.serialize())
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        eof_self_queue.start_consuming(on_eof_message)


    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)

            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"EOF recibido en Self EOF Queue | request_id: {message.request_id}")
                self.eof_out_exchange.send(message.serialize(), str(message.type))
                return

            self._ensure_request(message.request_id)

            self._inc_inflight(message.request_id)

            items = message.process_message()

            if not items:
                logging.info(f"No hay items en el mensaje | request_id: {message.request_id} | type: {message.type}")
                return

            new_chunk = '' 
            for item in items:
                item_time = item.get_time()
                time = item_time.hour
                if time > min(self.time) and time < max(self.time):
                    new_chunk += item.serialize()
            
            if new_chunk:
                message.update_content(new_chunk)
                serialized = message.serialize()
                self.data_out_exchange.send(serialized, str(message.type))
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

        finally:
            self._dec_inflight(message.request_id)

    def close(self):
        try:
            self.data_input_queue.close()
            self.data_out_exchange.close()
            self.eof_out_exchange.close()
        except Exception as e:
            logging.error(f"Error al cerrar las conexiones: {type(e).__name__}: {e}")


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
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "eof_queue_1": os.getenv('EOF_QUEUE_NODO_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_NODO_2'),
        "output_exchange_filter_time": os.getenv('EXCHANGE_NAME'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO')
    }

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

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])

    data_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_filter_time"], 
                                        {config_params["output_queue_1"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                        config_params["output_queue_2"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)]})

    eof_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["eof_exchange_name"],
                                        {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                                         config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]})


    filter = FilterTimeNode(data_input_queue, data_output_exchange, eof_exchange, config_params["rabbitmq_host"], config_params["eof_service_queue"],  config_params["eof_self_queue"], {6, 23})
    filter.start()

if __name__ == "__main__":
    main()
