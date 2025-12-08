from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from multiprocessing import Process
from Middleware.connection import PikaConnection
from utils.heartbeat import start_heartbeat_sender

class FilterYearNode(Worker):

    def __init__(self,
                 data_input_queue: str,
                 data_output_exchange: str,
                 output_exchange_queues: dict,
                 sharding_q4_amount: int,
                 sharding_q2_amount: int,
                 eof_output_exchange: str,
                 eof_output_queues: dict,
                 eof_self_queue: str,
                 eof_service_queue: str,
                 eof_final_queue: str,
                 container_name: str,
                 host: str,
                 years_set):
        self.__init_manager__()
        self.__init_middlewares_handler__()
     
        self.connection = PikaConnection(host)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.output_exchange_queues = output_exchange_queues
        self.sharding_q4_amount = sharding_q4_amount
        self.sharding_q2_amount = sharding_q2_amount
        self.eof_output_exchange = eof_output_exchange
        self.eof_output_queues = eof_output_queues
        self.eof_service_queue = eof_service_queue
        self.eof_self_queue = eof_self_queue
        self.eof_final_queue = eof_final_queue
        self.node_id = container_name
        self.years = years_set

    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_data_queue()
        self._consume_eof()
        self._consume_eof_final()
        self.connection.start_consuming()
        
    def _consume_eof_final(self):
        eof_final_queue = MessageMiddlewareQueue(self.eof_final_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, self.output_exchange_queues, self.connection)
        
        def __on_eof_final_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: final EOF message received | request_id: {message.request_id}")  
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()          
                    data_output_exchange.send(message.serialize(), str(message.type))
            except Exception as e:
                logging.error(f"action: ERROR processing final EOF message | error: {type(e).__name__}: {e}")
        
        eof_final_queue.start_consuming(__on_eof_final_message__)

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, self.output_exchange_queues, self.connection)
        eof_exchange = MessageMiddlewareExchange(self.eof_output_exchange, self.eof_output_queues, self.connection)

        def __on_message__(message_bytes):
            
            message = None
            
            try:
                message = Message.deserialize(message_bytes)
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
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
                    if message.type == MESSAGE_TYPE_TRANSACTIONS:
                        sharding_key_q4 = message.msg_num % self.sharding_q4_amount
                        data_output_exchange.send(message.serialize(), f"{str(message.type)}.q4.{sharding_key_q4}")
                    elif message.type == MESSAGE_TYPE_TRANSACTION_ITEMS:
                        sharding_key_q2 = message.msg_num % self.sharding_q2_amount
                        data_output_exchange.send(message.serialize(), f"{str(message.type)}.q2.{sharding_key_q2}")
                    else:
                        data_output_exchange.send(message.serialize(), str(message.type))

            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)

        data_input_queue.start_consuming(__on_message__)
        
    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            logging.error(f"action: ERROR during middleware closing | error: {type(e).__name__}: {e}")
            

def initialize_config():
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue_1": os.getenv('OUTPUT_QUEUE_1'),
        "output_exchange_filter_year": os.getenv('EXCHANGE_NAME'),
        "eof_exchange_name": os.getenv('EOF_EXCHANGE_NAME'),
        "eof_self_queue": os.getenv('EOF_SELF_QUEUE'),
        "eof_queue_1": os.getenv('EOF_QUEUE_1'),
        "eof_queue_2": os.getenv('EOF_QUEUE_2'),
        "eof_service_queue": os.getenv('EOF_SERVICE_QUEUE'),
        "eof_final_queue": os.getenv('EOF_FINAL_QUEUE'),
        "container_name": os.getenv('CONTAINER_NAME'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "years": os.getenv('FILTER_YEARS'),
    }
    
    q4_queues = []
    while True:
        q_name = os.getenv(f'OUTPUT_QUEUE_Q4_{len(q4_queues)+1}')
        if q_name is None:
            break
        q4_queues.append(q_name)
        
    config_params["output_queues_q4"] = q4_queues
    
    q2_queues = []
    while True:
        q_name = os.getenv(f'OUTPUT_QUEUE_Q2_{len(q2_queues)+1}')
        if q_name is None:
            break
        q2_queues.append(q_name)
        
    config_params["output_queues_q2"] = q2_queues

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue_1",
        "output_exchange_filter_year",
        "eof_exchange_name",
        "eof_self_queue",
        "eof_queue_1",
        "eof_queue_2",
        "eof_service_queue",
        "eof_final_queue",
        "container_name",
        "years",
    ]
    
    missing = [k for k in required_keys if config_params[k] is None]
    if missing:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing)}. Aborting filter.")
    
    if not config_params["output_queues_q4"]:
         raise ValueError("Expected at least one Q4 output queue. Aborting filter.")
    
    if not config_params["output_queues_q2"]:
         raise ValueError("Expected at least one Q2 output queue. Aborting filter.")
    return config_params


def main():
    config = initialize_config()
    initialize_log(config["logging_level"])

    output_exchange_queues =  {config["output_queue_1"]: [f"{str(MESSAGE_TYPE_TRANSACTIONS)}.#", str(MESSAGE_TYPE_EOF)]}
    
    index = 0
    for queue in config["output_queues_q4"]:
        output_exchange_queues[queue] = [f"{str(MESSAGE_TYPE_TRANSACTIONS)}.q4.{index}", str(MESSAGE_TYPE_EOF)]
        index += 1
    
    index = 0
    for queue in config["output_queues_q2"]:
        output_exchange_queues[queue] = [f"{str(MESSAGE_TYPE_TRANSACTION_ITEMS)}.q2.{index}", str(MESSAGE_TYPE_EOF)]
        index += 1
    
    sharding_q4_amount = len(config["output_queues_q4"])
    sharding_q2_amount = len(config["output_queues_q2"])
   
    eof_output_queues = {config["eof_queue_1"]: [str(MESSAGE_TYPE_EOF)],
                            config["eof_queue_2"]: [str(MESSAGE_TYPE_EOF)]}
    
    years_set = set(map(int, config["years"].split(',')))
    logging.info(f"action: filtering years | years: {years_set}")
    node = FilterYearNode(
        config["input_queue"],
        config["output_exchange_filter_year"],
        output_exchange_queues,
        sharding_q4_amount,
        sharding_q2_amount,
        config["eof_exchange_name"],
        eof_output_queues,
        config["eof_self_queue"],
        config["eof_service_queue"],
        config["eof_final_queue"],
        config["container_name"],
        config["rabbitmq_host"],
        years_set,
    )
    
    node.start()

if __name__ == "__main__":
    main()
