from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
from pkg.message.q2_result import Q2IntermediateResult
import os
from multiprocessing import Process, Value
from utils.heartbeat import start_heartbeat_sender


class AggregatorMonth(Worker):
    
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
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.host = host
        self.eof_self_queue= eof_self_queue
        self.eof_service_queue = eof_service_queue
    
    def start(self):
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()

    def _consume_data_queue(self):
        eof_out_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        data_out_queue = MessageMiddlewareQueue(self.host, self.data_output_queue)
        data_in_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        self.message_middlewares.extend([eof_out_exchange, data_out_queue, data_in_queue])
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_out_exchange.send(message.serialize(), str(message.type))
                    return

                logging.debug(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)

                items = message.process_message()
                groups = self._group_items_by_month(items)
                new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT, message.msg_num, '')
                self._send_groups(new_message, groups, data_out_queue)
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            finally:
                self._dec_inflight(message.request_id)
            
        data_in_queue.start_consuming(__on_message__, prefetch_count=1)

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q2IntermediateResult(f"{year}-0{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    
    def close(self):
        try:
            for middleware in self.message_middlewares:
                middleware.close()
            self.heartbeat_sender.stop()
            self.message_middlewares = []
            logging.info(f"action=close_connections | status=success")
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

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["eof_self_queue"] = os.getenv('EOF_SELF_QUEUE')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["eof_exchange_name"] = os.getenv('EOF_EXCHANGE_NAME')
    config_params["eof_queue_1"] = os.getenv('EOF_QUEUE_1')
    config_params["eof_queue_2"] = os.getenv('EOF_QUEUE_2')
    config_params["eof_service_queue"] = os.getenv('EOF_SERVICE_QUEUE')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["logger_name"] = os.getenv('CONTAINER_NAME')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    eof_output_queues = {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                            config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}

    aggregator = AggregatorMonth(config_params["input_queue"], 
                                config_params["output_queue"], 
                                config_params["eof_exchange_name"], 
                                eof_output_queues,
                                config_params["eof_self_queue"],
                                config_params["eof_service_queue"],
                                config_params["rabbitmq_host"])
    aggregator.start()

if __name__ == "__main__":
    main()