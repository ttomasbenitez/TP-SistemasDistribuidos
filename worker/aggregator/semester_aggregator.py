from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT
from multiprocessing import Process, Value
from utils.heartbeat import start_heartbeat_sender


class SemesterAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 eof_output_exchange: str,
                 eof_output_queues: dict, 
                 eof_self_queue: str,
                 eof_service_queue: str,
                 host: str):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.host = host
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue

    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_queue = MessageMiddlewareQueue(self.host, self.data_output_queue)
        eof_output_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_input_queue, data_output_queue, eof_output_exchange])
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_output_exchange.send(message.serialize(), str(message.type))
                    return
                
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id) 

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
                    
                for period, total in agg.items():
                    res = Q3IntermediateResult(period, store_id, total)
                    self._send_grouped_item(message, res, data_output_queue)

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
        
        data_input_queue.start_consuming(__on_message__)

   
    def _send_grouped_item(self, message, item, data_output_queue):
        new_chunk = item.serialize()
        new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, message.msg_num, new_chunk)
        data_output_queue.send(new_message.serialize())

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
        "output_queue": os.getenv('OUTPUT_QUEUE_1'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_2'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
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
    
    eof_exchange_queues =  {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                                 config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    
    aggregator = SemesterAggregator(config_params["input_queue"], 
                                    config_params["output_queue"], 
                                    config_params["eof_exchange_name"], 
                                    eof_exchange_queues,
                                    config_params["eof_self_queue"],
                                    config_params["eof_service_queue"],  
                                    config_params["rabbitmq_host"])
    aggregator.start()

if __name__ == "__main__":
    main()
