from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from worker.joiner.joiner import Joiner 
from pkg.message.constants import (
    MESSAGE_TYPE_EOF,
    MESSAGE_TYPE_STORES,
    MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_3_RESULT,
)
from utils.custom_logging import initialize_log
import os
from pkg.message.q3_result import Q3Result
import hashlib
import threading

EXPECTED_EOFS = 3 # 1 stores, 2 semester

class StoresJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_exchange: str,  
                 stores_input_queue: str,
                 host: str):
        
        super().__init_client_handler__(stores_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_exchange = data_output_exchange
        self.pending_transactions = []
        self.processed_transactions = {}
        self._seen_lock = threading.Lock()
        self._seen_messages = set()

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            try:
                # Idempotencia robusta: deduplicar usando hash del cuerpo
                try:
                    raw_bytes = msg if isinstance(msg, (bytes, bytearray)) else str(msg).encode("utf-8")
                except Exception:
                    raw_bytes = message.serialize().encode("utf-8")
                body_hash = hashlib.sha256(raw_bytes).hexdigest()
                dedup_key = (message.request_id, message.type, body_hash)
                with self._seen_lock:
                    if dedup_key in self._seen_messages:
                        return
                    self._seen_messages.add(dedup_key)
                items = message.process_message()
                if message.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
                    for item in items:
                        store_name = self.items_to_join.get(message.request_id, {}).get(item.get_store())
                        if store_name:
                            key = (message.request_id, store_name, item.get_period())
                            self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
                        else:
                            self.pending_transactions.append((item, message.request_id))
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)

    def _process_items_to_join(self, message):
        items = message.process_message()
        if message.type == MESSAGE_TYPE_STORES:
            for item in items:
                if message.request_id not in self.items_to_join:
                    self.items_to_join[message.request_id] = {}
                self.items_to_join[message.request_id][item.get_id()] = item.get_name()
        logging.info(f"action: Stores updated | request_id: {message.request_id}")
                    
    def _send_results(self, message):
        data_output_exchange = MessageMiddlewareExchange(self.data_output_exchange, {}, self.connection)
        self.message_middlewares.append(data_output_exchange)
        self._process_pending()
        self._send_joined_transactions_by_request(message, message.request_id, data_output_exchange)
        self._send_eof(message, data_output_exchange)
 
    def _process_pending(self):
        for item, request_id in list(self.pending_transactions):
            store_name = self.items_to_join.get(request_id, {}).get(item.get_store())
            if store_name:
                key = (request_id, store_name, item.get_period())
                self.processed_transactions[key] = self.processed_transactions.get(key, 0.0) + item.get_tpv()
        
        self.pending_transactions = []

    def _send_joined_transactions_by_request(self, message, request_id, data_output_exchange):
        total_chunk = ''
        for (key, total_tpv) in self.processed_transactions.items():
            req_id, store_name, period = key
            if req_id != request_id:
                continue
            q3Result = Q3Result(period, store_name, total_tpv)
            total_chunk += q3Result.serialize()
        msg = Message(message.request_id, MESSAGE_TYPE_QUERY_3_RESULT, message.msg_num, total_chunk)
        data_output_exchange.send(msg.serialize(), str(message.request_id))
        logging.info(f"action: results sent | request_id: {message.request_id}")

    def _send_eof(self, message, data_output_exchange):
        data_output_exchange.send(message.serialize(), str(message.request_id))
        logging.info(f"EOF sent | request_id: {message.request_id}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_exchange_q3"] = os.getenv('OUTPUT_EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = StoresJoiner(
        config_params["input_queue_1"],
        config_params["output_exchange_q3"],
        config_params["input_queue_2"], 
        config_params["rabbitmq_host"])
    joiner.start()


if __name__ == "__main__":
    main()
