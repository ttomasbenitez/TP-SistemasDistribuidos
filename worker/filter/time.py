from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_TRANSACTIONS
from multiprocessing import Process
from utils.heartbeat import start_heartbeat_sender

import hashlib

class FilterTimeNode(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str, 
                 q1_output_queues: list,
                 q3_output_queue: str,
                 eof_output_exchange: str,
                 eof_output_queues: dict,
                 eof_self_queue: str,
                 eof_service_queue: str,
                 host: str, 
                 time_set):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.q1_output_queues = q1_output_queues
        self.q3_output_queue = q3_output_queue
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.host = host
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue
        self.time = time_set

    def start(self):
       
        logging.info(f"Starting process")
        p_data = Process(target=self._consume_data_queue)
        
        logging.info(f"Starting EOF node process")
        p_eof = Process(target=self._consume_eof)
        
        self.heartbeat_sender = start_heartbeat_sender()

        for p in (p_data, p_eof): p.start()
        for p in (p_data, p_eof): p.join()
        
    def _consume_data_queue(self):
        data_in_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        
        q1_queues = [MessageMiddlewareQueue(self.host, q) for q in self.q1_output_queues]
        q3_queue = MessageMiddlewareQueue(self.host, self.q3_output_queue)
        
        eof_output_exchange = MessageMiddlewareExchange(self.host, self.eof_output_exchange, self.eof_output_queues)
        self.message_middlewares.extend([data_in_queue, q3_queue, eof_output_exchange] + q1_queues)
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    eof_output_exchange.send(message.serialize(), str(message.type))
                    
                    # Broadcast EOF to all Q1 queues
                    for q in q1_queues:
                        q.send(message.serialize())
                    
                    # Send EOF to Q3 queue
                    q3_queue.send(message.serialize())
                    return

                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)

                items = message.process_message()

                if not items:
                    logging.info(f"action: no more items to process | request_id: {message.request_id} | type: {message.type}")
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
                    
                    # Send to Q3 (broadcast/single)
                    q3_queue.send(serialized)
                    
                    # Send to Q1 (sharded)
                    # Modulo msg_num to select queue
                    queue_index = message.msg_num % len(q1_queues)
                    q1_queues[queue_index].send(serialized)
                    
            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                self._dec_inflight(message.request_id)

        data_in_queue.start_consuming(__on_message__)

def initialize_config():
    """ Parse env variables to find program config params
    """
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "q3_output_queue": os.getenv('OUTPUT_QUEUE_Q3'),
        "eof_queue_1": os.getenv('EOF_QUEUE_NODO_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_NODO_2'),
        "output_exchange_filter_time": os.getenv('EXCHANGE_NAME'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO')
    }
    
    # Read Q1 output queues
    q1_queues = []
    i = 1
    while True:
        queue = os.getenv(f'OUTPUT_QUEUE_Q1_{i}')
        if not queue:
            break
        q1_queues.append(queue)
        i += 1
    config_params["q1_output_queues"] = q1_queues

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "q3_output_queue",
        "output_exchange_filter_time",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    if not config_params["q1_output_queues"]:
         raise ValueError("Expected at least one Q1 output queue. Aborting filter.")
    
    return config_params


def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    # output_exchange_queues no longer used for data, but maybe for EOF? No, EOF uses eof_output_queues
    
    eof_output_queues = {config_params["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                        config_params["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}

    filter = FilterTimeNode(config_params["input_queue"], 
                            config_params["output_exchange_filter_time"],
                            config_params["q1_output_queues"],
                            config_params["q3_output_queue"],
                            config_params["eof_exchange_name"],
                            eof_output_queues,
                            config_params["eof_self_queue"],
                            config_params["eof_service_queue"],
                            config_params["rabbitmq_host"], {6, 23})
    filter.start()

if __name__ == "__main__":
    main()
