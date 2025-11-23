from pkg.message.q4_result import Q4Result
from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_STORES
from utils.custom_logging import initialize_log
import os

EXPECTED_EOFS = 2

class Q4StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str, 
                 data_output_exchange: str, 
                 stores_input_queue: str,
                 host: str):
        super().__init_client_handler__(stores_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.pending_clients = {}
        self.processed_clients = dict()
        
    def _process_items_to_join(self, message):
        items = message.process_message()
        
        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                key = (item.get_id(), message.request_id)
                with self.items_to_join_lock:
                    self.items_to_join[key] = item.get_name()
        logging.debug(f"action: Stores updated | request_id: {message.request_id}")
                    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.host, self.data_output_exchange, {})
        self.message_middlewares.append(data_output_exchange)
        self._process_pending(request_id=message.request_id)
        self._send_processed_clients(message, data_output_exchange)
        self._send_eof(message, data_output_exchange)
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.host, self.data_input_queue)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.debug(f"action: message received | request_id: {message.request_id} | type: {message.type}")
            
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            items = message.process_message()
            
            if message.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
                for item in items:
                    with self.items_to_join_lock:
                        store_name = self.items_to_join.get((item.get_store(), message.request_id), 0)
                    if store_name:
                        self.processed_clients.setdefault(message.request_id, []).append(Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()))
                    else:
                        self.pending_clients[(item.get_store(), message.request_id)] = item
                        
        data_input_queue.start_consuming(__on_message__)

    def _send_processed_clients(self, message, data_output_exchange):
        total_chunk = ''
        results = self.processed_clients.get(message.request_id, [])
        for q4Result in results:
            total_chunk += q4Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_4_RESULT, message.msg_num, total_chunk)
        data_output_exchange.send(msg.serialize(), str(message.request_id))

    def _process_pending(self, request_id):
        for (store, req_id), item in self.pending_clients.items():
            if req_id == request_id:
                store_name = self.items_to_join.get((store, req_id))
                if store_name:
                    self.processed_clients.setdefault(req_id, []).append(Q4Result(store_name, item.get_birthdate(), item.get_purchases_qty()))
        
        self.pending_clients = {k: v for k, v in self.pending_clients.items() if v.request_id != request_id}

    def _send_eof(self, message, data_output_exchange):
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q4"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    
    joiner = Q4StoresJoiner(config_params["input_queue_1"], 
                            config_params["output_exchange_q4"],
                            config_params["input_queue_2"],
                            config_params["rabbitmq_host"])
    joiner.start()

if __name__ == "__main__":
    main()
