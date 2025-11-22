from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from multiprocessing import Process
from utils.heartbeat import start_heartbeat_sender

class FilterYearNode(Worker):

    def __init__(self,
                 data_input_queue: str,
                 data_output_exchange: str,
                 output_exchange_queues: dict,
                 eof_output_exchange: str,
                 eof_output_queues: dict,
                 eof_self_queue: str,
                 eof_service_queue: str,
                 eof_final_queue: str,
                 host: str,
                 years_set):
        self.__init_manager__()
        self.__init_middlewares_handler__()
     
        self.host = host
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.output_exchange_queues = output_exchange_queues
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue
        self.eof_final_queue = eof_final_queue
        self.years = years_set

    def start(self):
        logging.info("Starting data consumer process")
        p_data = Process(target=self._consume_data_queue)

        logging.info("Starting EOF consumer process")
        p_eof = Process(target=self._consume_eof)
        
        logging.info(f"Starting EOF FINAL process")
        p_eof_final = Process(target=self._consume_eof_final)

        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof, p_eof_final):
            p.start()
        for p in (p_data, p_eof, p_eof_final):
            p.join()

    def _consume_eof_final(self):
        eof_final_queue = MessageMiddlewareQueue(self.host, self.eof_final_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, self.output_exchange_queues)
        self.message_middlewares.extend([eof_final_queue, data_output_exchange])
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: final EOF message received | request_id: {message.request_id}")            
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    data_output_exchange.send(message.serialize(), str(message.type))
            except Exception as e:
                logging.error(f"Error al procesar el mensaje EOF FINAL: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, self.output_exchange_queues)
        eof_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_input_queue, data_output_exchange, eof_exchange])

        def __on_message__(message_bytes):
            
            message = None
            
            try:
                message = Message.deserialize(message_bytes)
                logging.debug(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_exchange.send(message.serialize(), str(message.type))
                    return

                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)
                
                items = message.process_message()
                if not items:
                    logging.info(f"action: no more items to process | request_id: {message.request_id} | type: {message.type}")
                    return

                new_chunk = ''.join(it.serialize() for it in items if it.get_year() in self.years)
                if new_chunk:
                    message.update_content(new_chunk)
                    data_output_exchange.send(message.serialize(), str(message.type))

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                if message is not None:
                    self._dec_inflight(message.request_id)

        data_input_queue.start_consuming(__on_message__, prefetch_count=1)

def initialize_config():
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_queue_2": os.getenv('OUTPUT_QUEUE_2'),
        "output_queue_3": os.getenv('OUTPUT_QUEUE_3'),
        "output_exchange_filter_year": os.getenv('EXCHANGE_NAME'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_2'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_queue_2",
        "output_queue_3",
        "output_exchange_filter_year",
    ]
    missing = [k for k in required_keys if config_params[k] is None]
    if missing:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing)}. Aborting filter.")
    return config_params


def main():
    config = initialize_config()
    initialize_log(config["logging_level"])

   # TODO: hacer esto dinamico en base a config
    output_exchange_queues =  {config["output_queue_1"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                config["output_queue_2"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                config["output_queue_3"]: [str(MESSAGE_TYPE_TRANSACTION_ITEMS), str(MESSAGE_TYPE_EOF)]}
    eof_output_queues = {config["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                            config["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    node = FilterYearNode(
        config["input_queue"],
        config["output_exchange_filter_year"],
        output_exchange_queues,
        config["eof_exchange_name"],
        eof_output_queues,
        config["eof_self_queue"],
        config["eof_service_queue"],
        config["eof_final_queue"],
        config["rabbitmq_host"],
        years_set={2024, 2025},
    )
    node.start()

if __name__ == "__main__":
    main()
