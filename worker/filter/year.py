from worker.base import Worker 
from worker.eof.eof_service import EofService
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log, setup_process_logger
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from multiprocessing import Process


class FilterYearNode(Worker):
        
    def __init__(self, host, in_queue_name, 
                data_out_exchange: MessageMiddlewareExchange, 
                eof_exchange: MessageMiddlewareExchange, 
                eof_queue: MessageMiddlewareQueue,
                eof_service: EofService,
                eof_final_queue: MessageMiddlewareQueue, 
                years_set):
        self.__init_manager__()
        self.host = host
        self.in_queue_name = in_queue_name
        # self.data_in_queue = data_in_queue
        self.data_out_exchange = data_out_exchange
        self.years = years_set
        self.eof_exchange = eof_exchange
        self.eof_queue = eof_queue
        self.eof_service = eof_service
        self.eof_final_queue = eof_final_queue

    def start(self):
        # Creamos un proceso por cada input queue
        p_eof_service = Process(target=self._consume_eof_service,args=(self.eof_service,))

        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof, args=(self.eof_queue,))
        
        logging.info(f"Starting final EOF consume process")
        p_end = Process(target=self._consume_final_eof, args=(self.eof_final_queue,))

        # Esperamos que terminen
        for p in (p_data, p_eof, p_end, p_eof_service): p.start()
        for p in (p_data, p_eof, p_end, p_eof_service): p.join()
        
    def _consume_eof_service(self, eof_service: EofService):
        eof_service.start()

    def _consume_final_eof(self, queue: MessageMiddlewareQueue):
        setup_process_logger('name=filter_year_node', level=logging.getLevelName(logging.getLogger().level))
        def _on_final_eof(message):
            try:     
                message = Message.deserialize(message)
                self.data_out_exchange.send(message.serialize(), str(message.type))
                logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")
            except Exception as e:
                logging.error(f"Error enviando EOF: {e}")
        queue.start_consuming(_on_final_eof)
    
    def _consume_eof(self, queue: MessageMiddlewareQueue): 
        setup_process_logger('name=filter_year_node', level=logging.getLevelName(logging.getLogger().level))
        def on_eof_message(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en nodo | request_id: {message.request_id} | type: {message.type}")
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    self.eof_service.send_message(message.serialize())
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        queue.start_consuming(on_eof_message)
        
    def _consume_data_queue(self):
        setup_process_logger('name=filter_year_node', level="INFO")
        queue = MessageMiddlewareQueue(self.host, self.in_queue_name)          # <-- NUEVO en el hijo
        try:
            queue.start_consuming(self.__on_message__)
        finally:
            queue.close()

    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            
            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"EOF recibido en data queue | request_id: {message.request_id}")
                self.eof_exchange.send(message.serialize(), str(message.type))
                return
            
            self._ensure_request(message.request_id)
            
            self._inc_inflight(message.request_id) 
            
            items = message.process_message()

            if not items:
                logging.info(f"No hay items en el mensaje | request_id: {message.request_id} | type: {message.type}")
                return

            new_chunk = '' 
            for item in items:
                year = item.get_year()
                if year in self.years:
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
            self.data_in_queue.close()
            self.data_out_exchange.close()
            self.eof_exchange.close()
            self.eof_service.close()
            self.eof_final_queue.close()
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
        "output_queue_3": os.getenv('OUTPUT_QUEUE_3'),
        "output_exchange_filter_year": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "expected_acks": int(os.getenv('EXPECTED_ACKS')),
        "eof_queue": os.getenv('EOF_QUEUE'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
        "eof_exchange": os.getenv('EOF_EXCHANGE_NAME'),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_queue_3",
        "output_exchange_filter_year",
        "expected_acks",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    data_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_filter_year"], 
                                        {config_params["output_queue_1"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                        config_params["output_queue_2"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                        config_params["output_queue_3"]: [str(MESSAGE_TYPE_TRANSACTION_ITEMS), str(MESSAGE_TYPE_EOF)]})

    eof_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["eof_exchange"], 
                                        {config_params["eof_queue"]: [str(MESSAGE_TYPE_EOF)]})
    eof_service = EofService(config_params["expected_acks"], 
                             MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["eof_service_queue"]), 
                             MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["eof_final_queue"]))
    eof_final_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["eof_final_queue"])
    eof_node_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["eof_queue"])
    # filter = FilterYearNode(data_input_queue, data_output_exchange, eof_exchange, eof_node_queue, eof_service, eof_final_queue, {2024, 2025})
    host = config_params["rabbitmq_host"]
    in_queue_name = config_params["input_queue"]
    filter = FilterYearNode(host, in_queue_name, data_output_exchange, eof_exchange, eof_node_queue, eof_service, eof_final_queue, {2024, 2025})
    filter.start()

if __name__ == "__main__":
    main()
