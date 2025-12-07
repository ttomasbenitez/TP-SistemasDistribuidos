
from worker.base import Worker 
from abc import ABC, abstractmethod
from utils.heartbeat import start_heartbeat_sender
import logging
from pkg.message.message import Message
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from pkg.message.constants import MESSAGE_TYPE_EOF
from Middleware.connection import PikaConnection
from pkg.dedup.dedup_by_sender_strategy import DedupBySenderStrategy
from pkg.storage.state_storage.base import StateStorage
class Joiner(Worker, ABC):
    
    def __init__(self, 
                 data_input_queue: str, 
                 items_input_queue: str,
                 data_output_middleware: str,
                 state_storage: StateStorage,
                 container_name :str,
                 query_num: int,
                 host: str,
                 expected_eofs):
        
        self.data_input_queue = data_input_queue
        self.data_output_middleware = data_output_middleware
        self.items_input_queue = items_input_queue
        self.node_id = container_name
        self.connection = PikaConnection(host)
        self.state_storage = state_storage
        self.dedup_strategy = DedupBySenderStrategy(self.state_storage)
        self.expected_eofs = expected_eofs
        self.query_num = query_num
        
    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
        self.state_storage.load_state_all()
        self.connection.start()

        index = 0
        for data in self.state_storage.data_by_request:
            if data[index]["last_eof_count"] == self.expected_eofs:
                self.consume_items_to_join_queue(index)
                index += 1
        self.consume_data_queue()
        self.connection.start_consuming()

    def consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)

        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received from data_queue | request_id: {message.request_id} | type: {message.type}")
            
            if message.type == MESSAGE_TYPE_EOF:
                if self.on_eof_message(message, self.dedup_strategy, self.state_storage, self.expected_eofs):
                    logging.info(f"action: EOF message processed | request_id: {message.request_id} | type: {message.type}")
                    self.consume_items_to_join_queue(message.request_id)
                return
            
            if self.dedup_strategy.is_duplicate(message):
                logging.info(f"action: duplicate message discarded | request_id: {message.request_id} | type: {message.type} | sender_id: {message.node_id} | msg_num: {message.msg_num}")
                return
            
            try:
                items = message.process_message() 
                request_id = message.request_id
                state = self.state_storage.get_state(request_id)
                pending_results = state.get("pending_results", [])
                pending_results.extend(items)
                state['pending_results'] = pending_results
            finally:
                self.state_storage.save_state(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)


    def _send_eof(self, message: Message):
        
        data_output_middleware = MessageMiddlewareExchange(self.data_output_middleware, {}, self.connection)
        current_state = self.state_storage.get_state(message.request_id)
        current_msg_num = current_state.get("current_msg_num", -1)
        new_msg_num = current_msg_num + 1
        new_eof = Message(message.request_id, MESSAGE_TYPE_EOF, new_msg_num, self.query_num, self.node_id)
        data_output_middleware.send(new_eof.serialize(), str(message.request_id))
        self.state_storage.delete_state(message.request_id)
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")

    def consume_items_to_join_queue(self, request_id: int):
        items_input_queue = MessageMiddlewareQueue(f"{self.items_input_queue}.{request_id}", self.connection)
        
        def __on_items_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
             
            if message.type == MESSAGE_TYPE_EOF:
                self._send_eof(message)
                return
            
            self._process_items_to_join(message)
            logging.info(f"action: items to join processed | request_id: {message.request_id} | msg_type: {message.type}")
            # self.state_storage.save_state(message.request_id) TODO Ver si podemos reducir la cantidad de accesos a disco aca
            
        items_input_queue.start_consuming(__on_items_message__)
        
    @abstractmethod
    def _process_items_to_join(self, message: Message):
        pass