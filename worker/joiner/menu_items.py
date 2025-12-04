import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_QUERY_2_RESULT
from utils.custom_logging import initialize_log
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import os
from pkg.message.utils import parse_int



class JoinerMenuItems(Joiner):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str,
                 menu_items_input_queue: str,
                 host: str,
                 aggregator_quantity_profit_replicas: int = 2):
        # Expected EOFs: 1 from menu_items + N from aggregator-quantity-profit replicas
        expected_eofs = 1 + aggregator_quantity_profit_replicas
        super().__init_client_handler__(menu_items_input_queue, host, expected_eofs)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.pending_items = []
        self.eofs_sent_by_request = {}

    def _process_items_to_join(self, message):
        items = message.process_message()
        
        if message.type == MESSAGE_TYPE_MENU_ITEMS:
            for item in items:
                if message.request_id not in self.items_to_join:
                    self.items_to_join[message.request_id] = {}
                self.items_to_join[message.request_id][item.get_id()] = item.get_name()
        
        logging.info(f"action: Menu Items updated | request_id: {message.request_id}")
        
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._send_processed_clients(message, data_output_exchange)
        self._send_eof(message, data_output_exchange)
        
    
    def _send_processed_clients(self, message, data_output_exchange):
        
        request_id = message.request_id
        ready_to_send = ''
        
        for (item, req_id) in list(self.pending_items):
            if req_id != request_id:
                continue
            name = self.items_to_join.get(parse_int(item.item_data))
            if name:
                item.join_item_name(name)
                ready_to_send += item.serialize()
                self.pending_items.remove((item, req_id))

        if ready_to_send:
            serialized = Message(request_id, MESSAGE_TYPE_QUERY_2_RESULT, 0, ready_to_send).serialize()
            data_output_exchange.send(serialized, str(request_id))
        

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.extend([data_input_queue, data_output_exchange])
        
        def __on_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            try:
                items = message.process_message()
                if message.type == MESSAGE_TYPE_QUERY_2_RESULT:
                    ready_to_send = ''
                    for item in items:
                        name = self.items_to_join.get(message.request_id, {}).get(parse_int(item.item_data))
                        if name:
                            item.join_item_name(name)
                            ready_to_send += item.serialize()
                        else:
                            self.pending_items.append((item, message.request_id))

                    if ready_to_send:
                        message.update_content(ready_to_send)
                        serialized = message.serialize()
                        data_output_exchange.send(serialized, str(message.request_id))
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
                    
        data_input_queue.start_consuming(__on_message__)
        
    def _send_eof(self, message, data_output_exchange):
        # Send EOF only once per request_id, even if called multiple times
        if message.request_id in self.eofs_sent_by_request:
            logging.debug(f"action: EOF already sent | request_id: {message.request_id}")
            return
        
        message.update_content("2")
        data_output_exchange.send(message.serialize(), str(message.request_id))
        self.eofs_sent_by_request[message.request_id] = True
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")


                       
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
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q2"] = os.getenv('OUTPUT_EXCHANGE')
    config_params["aggregator_quantity_profit_replicas"] = int(os.getenv('AGGREGATOR_QUANTITY_PROFIT_REPLICAS', '2'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in (config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_exchange_q2"]):
        raise ValueError("Expected value not found. Aborting.")
    
    return config_params

def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = JoinerMenuItems(config_params["input_queue_1"],
                                 config_params["output_exchange_q2"],
                                 config_params["input_queue_2"],
                                 config_params["rabbitmq_host"],
                                 config_params["aggregator_quantity_profit_replicas"])
    joiner.start()

if __name__ == "__main__":
    main()
