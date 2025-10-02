from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
from pkg.message.q4_result import Q4IntermediateResult
import os

class AggregatorMonth(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
        
    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                self.__received_EOF__(message)
                return
            
            items = message.process_message()
            groups = self._group_items_by_month(items)
            new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, message.msg_num, '')
            self._send_groups(new_message, groups, self.out_queue)
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
    
    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q4IntermediateResult(f"{year}-{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    

    def close(self):
        try:
            self.in_queue.close()
            self.out_queue.close()
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

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    
    aggregator = AggregatorMonth(input_queue, output_queue)
    aggregator.start()

if __name__ == "__main__":
    main()