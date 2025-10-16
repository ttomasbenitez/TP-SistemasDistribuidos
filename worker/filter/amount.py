from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.q1_result import Q1Result
from utils.custom_logging import initialize_log, setup_process_logger
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, QUERY_1, MESSAGE_TYPE_QUERY_1_RESULT
from multiprocessing import Process, Manager, Value, Lock

class FilterAmountNode(Worker):
    
    def __init__(self, data_input_queue: MessageMiddlewareQueue, data_out_exchange: MessageMiddlewareExchange,
                 eof_exchange: MessageMiddlewareExchange, host: str, data_out_queue_prefix: str,
                 eof_service_queue: str, eof_self_queue: str, eof_final_queue: str, amount_to_filter: int):
        
        self.__init_manager__()
        super().__init__(data_input_queue, host, eof_self_queue, eof_service_queue)
        self.data_out_exchange = data_out_exchange
        self.eof_out_exchange = eof_exchange
        self.data_out_queue_prefix = data_out_queue_prefix
        self.eof_final_queue = eof_final_queue
        self.clients = []
        self.amount_to_filter = amount_to_filter
        
    
    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue, args=(self.in_middleware,))
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        logging.info(f"Starting EOF FINAL process")
        p_eof_final = Process(target=self._consume_eof_final)
        # Esperamos que terminen
        for p in (p_data, p_eof, p_eof_final): p.start()
        for p in (p_data, p_eof, p_eof_final): p.join()

    def _consume_data_queue(self, queue: MessageMiddlewareQueue):
        setup_process_logger('name=filter_amount_node', level="INFO")
        queue.start_consuming(self.__on_message__)
        
    def _consume_eof_final(self):
        setup_process_logger('name=filter_amount_node', level="INFO")
        eof_final_queue = MessageMiddlewareQueue(self.host, self.eof_final_queue)
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF FINAL recibido en Self EOF Queue | request_id: {message.request_id}")            
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    self.data_out_exchange.send(message.serialize(), str(message.request_id))
                    logging.info(f"EOF FINAL enviado | request_id: {message.request_id} | type: {message.type}")
            except Exception as e:
                logging.error(f"Error al procesar el mensaje EOF FINAL: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)

            if message.request_id not in self.clients:
                out_queue_name = f"{self.data_out_queue_prefix}_{message.request_id}"
                self.data_out_exchange.add_queue_to_exchange(out_queue_name, str(message.request_id))
                self.clients.append(message.request_id)
            
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
                amount = item.get_final_amount()
                if amount >= self.amount_to_filter:
                    new_chunk += Q1Result(item.transaction_id, amount).serialize()
            if new_chunk:
                new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_1_RESULT, message.msg_num, new_chunk)
                serialized = new_message.serialize()
                self.data_out_exchange.send(serialized, str(message.request_id))

        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

        finally:
            self._dec_inflight(message.request_id)
    
    def close(self):
        try:
            # for in_queue in self.eof_in_queues:
            #     in_queue.close()
            # for out_queue in self.eof_out_queues:
            #     out_queue.close()
            # self.data_in_queue.close()
            self.data_out_exchange.close()
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
        "out_queue_name": os.getenv('OUTPUT_QUEUE_1'),
        "exchange": os.getenv('EXCHANGE_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_NODO_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_NODO_2'),
        "eof_queue_3": os.getenv('EOF_QUEUE_NODO_3'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
    }
    
    required_keys = [
        "rabbitmq_host",
        "out_queue_name",
        "input_queue",
        "exchange",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    logging.info("Iniciando Filter Amount Node SIN ERROR ALGUNO")
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    logging.info(f"Configuración cargada RABBITMQHOSTAMOUNT: {config_params['rabbitmq_host']}")

    data_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["exchange"], {})
    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    eof_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["eof_exchange_name"],
                                        {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                                         config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)],
                                         config_params["eof_queue_3"]: [str(MESSAGE_TYPE_EOF)]})

    filter = FilterAmountNode(data_input_queue, data_output_exchange, eof_exchange,
                                  config_params["rabbitmq_host"], config_params["out_queue_name"], config_params["eof_service_queue"],  
                                  config_params["eof_self_queue"],config_params["eof_final_queue"], 75)
    filter.start()

if __name__ == "__main__":
    main()
