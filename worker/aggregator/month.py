from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from Middleware.connection import PikaConnection
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
from pkg.message.q2_result import Q2IntermediateResult
import os
from utils.heartbeat import start_heartbeat_sender
import hashlib
from pkg.dedup.sliding_window_dedup_strategy import SlidingWindowDedupStrategy


class AggregatorMonth(Worker):
    
    def __init__(self,
                 data_input_queue: str,
                 data_output_exchange: str,
                 output_exchange_queues: list,
                 sharding_q2_amount: int,
                 storage_dir: str,
                 host: str,
                 container_name: str):
        
        self.__init_middlewares_handler__()
        self.data_output_exchange = data_output_exchange
        self.data_input_queue = data_input_queue
        self.output_exchange_queues = output_exchange_queues
        self.sharding_q2_amount = sharding_q2_amount
        self.connection = PikaConnection(host)
        self.node_id = container_name 
        self.dedup_strategy = SlidingWindowDedupStrategy(2, storage_dir)
    
    def start(self):
        
        self.heartbeat_sender = start_heartbeat_sender()
        self.dedup_strategy.load_dedup_state()
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
       
    
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, self.output_exchange_queues, self.connection)

        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    new_msg_count = self.get_msg_count(message.request_id)
                    new_eof_message = Message(message.request_id, MESSAGE_TYPE_EOF, new_msg_count, '', self.node_id)
                    data_output_exchange.send(new_eof_message.serialize(), str(MESSAGE_TYPE_EOF))
                    self.dedup_strategy.save_dedup_state(message)
                    return

                
                if self.dedup_strategy.is_duplicate(message):
                    return
        
                items = message.process_message()
                groups = self._group_items_by_month(items)
                self._send_groups_sharded(message, groups, data_output_exchange)

                self.dedup_strategy.save_dedup_state(message)
                   
            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")

        data_input_queue.start_consuming(__on_message__)

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q2IntermediateResult(f"{year}-0{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    
    def _send_groups_sharded(self, original_message: Message, groups: dict, output_exchange: MessageMiddlewareExchange):
        for key, items in groups.items():
            hash_val = int(hashlib.sha256(key.encode()).hexdigest(), 16)
            queue_index = hash_val % self.sharding_q2_amount
            new_chunk = ''.join(item.serialize() for item in items)
            new_msg_count = self.get_msg_count(original_message.request_id)
            new_message =  Message(original_message.request_id, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT, new_msg_count, new_chunk, self.node_id)
            logging.info(f"action: sending group | key: {key} | to_queue_index: {queue_index} | request_id: {new_message.request_id} | type: {new_message.type} | msg_num: {new_msg_count} | node_id: {self.node_id}")
            serialized = new_message.serialize()
            output_exchange.send(serialized, f"{MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT}.q2.{queue_index}")
            

    def get_msg_count(self, request_id: int) -> int:
        current = self.dedup_strategy.current_msg_num.get(request_id, -1)
        new_val = current + 1
        self.dedup_strategy.current_msg_num[request_id] = new_val
        return new_val

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
        "storage_dir": os.getenv('STORAGE_DIR'),
        "container_name": os.getenv('CONTAINER_NAME'),
    }
    
    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "exchange",
        "container_name",
        "storage_dir",
    ]
    
    output_queues = []
    i = 1
    while True:
        queue = os.getenv(f'OUTPUT_QUEUE_{i}')
        if not queue:
            break
        output_queues.append(queue)
        i += 1
    
    if not output_queues:
        pass

    config_params["output_queues"] = output_queues
    
    missing = [k for k in required_keys if config_params[k] is None]
    if missing:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing)}. Aborting filter.")
    
    if not config_params["output_queues"]:
         raise ValueError("Expected at least one Q2 output queue. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    output_exchange_queues = {}
    index = 0
    for queue in config_params["output_queues"]:
        output_exchange_queues[queue] = [f"{str(MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT)}.q2.{index}", str(MESSAGE_TYPE_EOF)]
        index += 1
    sharding_q2_amount = len(config_params["output_queues"])
    aggregator = AggregatorMonth(config_params["input_queue"], 
                                config_params["exchange"], 
                                output_exchange_queues,
                                sharding_q2_amount,
                                config_params["storage_dir"],
                                config_params["rabbitmq_host"],
                                config_params["container_name"])
    aggregator.start()

if __name__ == "__main__":
    main()