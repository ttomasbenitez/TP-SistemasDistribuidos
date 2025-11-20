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
                 data_in_queue: MessageMiddlewareQueue,
                 data_out_queue: MessageMiddlewareQueue,
                 eof_out_exchange: MessageMiddlewareExchange,
                 host: str,
                 eof_self_queue_name: str,
                 eof_service_queue_name: str):
        self.__init_manager__()
        self.data_in_queue = data_in_queue
        self.data_out_queue = data_out_queue
        self.eof_out_exchange = eof_out_exchange
        self.host = host
        self.eof_self_queue_name = eof_self_queue_name
        self.eof_service_queue_name = eof_service_queue_name
    
    def start(self):
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue, args=(self.data_in_queue,))
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        # Start Heartbeat in the main process
        self.heartbeat_sender = start_heartbeat_sender()

        # Esperamos que terminen
        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()

    def _consume_data_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self.__on_message__, prefetch_count=1)

    def _consume_eof(self): 
        eof_service_queue = MessageMiddlewareQueue(self.host, self.eof_service_queue_name)
        eof_self_queue = MessageMiddlewareQueue(self.host, self.eof_self_queue_name)

        def on_eof_message(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en EOF Queue Propia | request_id: {message.request_id} | type: {message.type}")
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
                logging.info(f"EOF recibido en data queue | request_id: {message.request_id}")
                self.eof_out_exchange.send(message.serialize(), str(message.type))
                return

            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)

            items = message.process_message()
            groups = self._group_items_by_month(items)
            new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT, message.msg_num, '')
            self._send_groups(new_message, groups)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

        finally:
            self._dec_inflight(message.request_id)

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q2IntermediateResult(f"{year}-0{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    
    def _send_groups(self, original_message, groups):
        for month, month_items in groups.items():
            new_chunk = ''.join(item.serialize() for item in month_items)
            new_message = original_message.new_from_original(new_chunk)
            serialized = new_message.serialize()
            self.data_out_queue.send(serialized)
            logging.info(f"Agregado correctamente | request_id: {new_message.request_id} | type: {new_message.type} | month: {month}")
    
    def close(self):
        try:
            self.data_in_queue.close()
            self.data_out_queue.close()
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

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])


    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])

    data_output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])

    eof_output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["eof_exchange_name"], 
                                        {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                                         config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]})

    aggregator = AggregatorMonth(data_input_queue, data_output_queue, eof_output_exchange, host=config_params["rabbitmq_host"],
                                eof_self_queue_name=config_params["eof_self_queue"],
                                eof_service_queue_name=config_params["eof_service_queue"])
    aggregator.start()

if __name__ == "__main__":
    main()