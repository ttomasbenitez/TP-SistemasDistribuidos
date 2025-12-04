from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message, Transaction
from utils.custom_logging import initialize_log
import os
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_TRANSACTIONS
from Middleware.connection import PikaConnection
from utils.heartbeat import start_heartbeat_sender
from pkg.dedup.sliding_window_dedup_strategy import SlidingWindowDedupStrategy
from pkg.message.utils import calculate_sub_message_id
from pkg.message.constants import SUB_MESSAGE_START_ID

class StoreAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str, 
                 output_exchange_queues: list,
                 data_output_exchange: str,
                 host: str,
                 total_shards: int,
                 storage_dir: str,
                 sharding_key: str,
                 node_number: int):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.output_exchange_queues = output_exchange_queues
        self.data_output_exchange = data_output_exchange
        self.connection = PikaConnection(host)
        self.total_shards = total_shards
        self.sharding_key = sharding_key
        self.node_id = node_number
        self.dedup_strategy = SlidingWindowDedupStrategy(total_shards, storage_dir)

    def start(self):    
        self.heartbeat_sender = start_heartbeat_sender()
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
        self.heartbeat_sender.stop()
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, self.output_exchange_queues, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        self.dedup_strategy.load_dedup_state()
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)

                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"action: EOF message received in data queue | request_id: {message.request_id}")
                    data_output_exchange.send(message.serialize(), str(message.type))
                    self.dedup_strategy.update_state_on_eof(message)
                    return
                
                logging.info(f"action: message received in data queue | request_id: {message.request_id} | msg_type: {message.type}")
                
                if self.dedup_strategy.check_state_before_processing(message) is False:
                    return
                items = message.process_message()
                groups = self._group_items_by_store(items)
                self._send_groups(message, groups, data_output_exchange)
                self.dedup_strategy.update_contiguous_sequence(message)
            except Exception as e:
                logging.error(f"action: ERROR processing message | error: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)

    def _group_items_by_store(self, items):
        groups = {}
        for item in items:
            store = item.get_store()
            groups.setdefault(store, []).append(item)
        return groups
    
    def _send_groups(self, original_message: Message, groups: dict, data_output_exchange: MessageMiddlewareExchange):
        current_msg_num = self.dedup_strategy.current_msg_num.get(original_message.request_id, 0)
        for key, items in groups.items():
            logging.info(f"action: sending grouped items | request_id: {original_message.request_id} | node: {self.node_id} | starting_msg_num: {current_msg_num}")
            new_chunk = ''.join(item.serialize() for item in items)
            new_message = Message(original_message.request_id, original_message.type, current_msg_num, new_chunk)
            new_message.add_node_id(self.node_id)
            logging.info(f'npde_id added: {self.node_id}')
            serialized = new_message.serialize()
            first_item = items[0]
            sharding_key_value = first_item.get_sharding_key(self.sharding_key)
            sharding_key = sharding_key_value % self.total_shards
            data_output_exchange.send(serialized, f"{str(new_message.type)}.{sharding_key}")
            current_msg_num += 1
        
        self.dedup_strategy.current_msg_num[original_message.request_id] = current_msg_num
               
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
        "output_exchange": os.getenv('OUTPUT_EXCHANGE'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "total_shards": int(os.getenv('TOTAL_SHARDS', 3)),
        "storage_dir": os.getenv('STORAGE_DIR', './data'),
        "sharding_key": os.getenv('SHARDING_KEY', 'request_id'),
        "node_number": int(os.getenv('NODE_NUMBER', 1)),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_exchange",
    ]
    
    queues = []
    while True:
        q_name = os.getenv(f'OUTPUT_QUEUE_{len(queues)+1}')
        if q_name is None:
            break
        queues.append(q_name)
        
    config_params["output_queues"] = queues
    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    output_exchange_queues =  {}
    
    index = 0
    for queue in config_params["output_queues"]:
        output_exchange_queues[queue] = [f"{str(MESSAGE_TYPE_TRANSACTIONS)}.{index}", str(MESSAGE_TYPE_EOF)]
        index += 1
    
    aggregator = StoreAggregator(config_params["input_queue"],
                                 output_exchange_queues,
                                 config_params["output_exchange"],
                                 config_params["rabbitmq_host"],
                                 config_params["total_shards"],
                                 config_params["storage_dir"],
                                 config_params["sharding_key"],
                                 config_params["node_number"])
    aggregator.start()

if __name__ == "__main__":
    main()
