from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.q1_result import Q1Result
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_1_RESULT
from multiprocessing import Process
from utils.heartbeat import start_heartbeat_sender

class FilterAmountNode(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 eof_output_exchange: str, 
                 eof_output_queues: str,
                 eof_self_queue: str, 
                 eof_service_queue: str, 
                 eof_final_queue: str, 
                 host: str, 
                 amount_to_filter: int):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.host = host
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_self_queue = eof_self_queue
        self.eof_service_queue = eof_service_queue
        self.eof_final_queue = eof_final_queue
        self.clients = []
        self.amount_to_filter = amount_to_filter
        
    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        logging.info(f"Starting EOF FINAL process")
        p_eof_final = Process(target=self._consume_eof_final)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof, p_eof_final): p.start()
        for p in (p_data, p_eof, p_eof_final): p.join()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        eof_output_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_input_queue, data_output_exchange, eof_output_exchange])
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_output_exchange.send(message.serialize(), str(message.type))
                    return

                logging.debug(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)

                items = message.process_message()
                if not items:
                    logging.info(f"action: no more items to process | request_id: {message.request_id} | type: {message.type}")
                    return
                
                new_chunk = '' 
                for item in items:
                    amount = item.get_final_amount()
                    if amount >= self.amount_to_filter:
                        new_chunk += Q1Result(item.transaction_id, amount).serialize()
                if new_chunk:
                    new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_1_RESULT, message.msg_num, new_chunk)
                    serialized = new_message.serialize()
                    data_output_exchange.send(serialized, str(message.request_id))

            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            finally:
                self._dec_inflight(message.request_id)

        data_input_queue.start_consuming(__on_message__)
        
    def _consume_eof_final(self):
        eof_final_queue = MessageMiddlewareQueue(self.host, self.eof_final_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        self.message_middlewares.extend([eof_final_queue, data_output_exchange])
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF FINAL recibido en Self EOF Queue | request_id: {message.request_id}")            
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    data_output_exchange.send(message.serialize(), str(message.request_id))
                    logging.info(f"EOF FINAL enviado | request_id: {message.request_id} | type: {message.type}")
            except Exception as e:
                logging.error(f"Error al procesar el mensaje EOF FINAL: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

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
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    eof_queues = {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)],
                config_params["eof_queue_3"]: [str(MESSAGE_TYPE_EOF)]}

    filter = FilterAmountNode(config_params["input_queue"], 
                            config_params["exchange"], 
                            config_params["eof_exchange_name"],
                            eof_queues,
                            config_params["eof_self_queue"],
                            config_params["eof_service_queue"],
                            config_params["eof_final_queue"],
                            config_params["rabbitmq_host"],  
                            75)
    filter.start()

if __name__ == "__main__":
    main()
