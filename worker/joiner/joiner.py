
from worker.base import Worker 
from abc import ABC, abstractmethod
import threading
from utils.heartbeat import start_heartbeat_sender
import logging
from pkg.message.message import Message
from Middleware.middleware import MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF
from Middleware.connection import PikaConnection

class Joiner(Worker, ABC):
  
    def __init_client_handler__(self, items_input_queue: str, host: str, expected_eofs: int):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.items_input_queue = items_input_queue
        self.connection = PikaConnection(host)
        self.expected_eofs = expected_eofs
        self.eofs_by_client = {}
        self.items_to_join = {}
        
    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
    
        self.connection.start()
        self._consume_data_queue()
        self._consume_items_to_join_queue()
        self.connection.start_consuming()

    @abstractmethod
    def _consume_data_queue(self):
        pass
    
    @abstractmethod
    def _process_items_to_join(self, message):
        pass
    
    @abstractmethod
    def _send_results(self, message):
        pass
    
    def _process_on_eof_message__(self, message):
        self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
        if self.eofs_by_client[message.request_id] < self.expected_eofs:
            logging.info(f"action: EOF message received {self.eofs_by_client[message.request_id]}/{self.expected_eofs} | request_id: {message.request_id} | type: {message.type}")
            return
        try:
            self._ensure_request(message.request_id)
            self.drained[message.request_id].wait()
            logging.info(f"action: EOF message received {self.eofs_by_client[message.request_id]}/{self.expected_eofs} send results | request_id: {message.request_id} | type: {message.type}")
            self._send_results(message)
        except Exception as e:
            logging.error(f"Error al procesar mensajes pendientes: {e}")
        return
    
    def _consume_items_to_join_queue(self):
        items_input_queue = MessageMiddlewareQueue(self.items_input_queue, self.connection)
        self.message_middlewares.append(items_input_queue)
        def __on_items_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
             
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            
            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            try:
                self._process_items_to_join(message)
            finally:
                if message.type != MESSAGE_TYPE_EOF:
                    self._dec_inflight(message.request_id)
            
        items_input_queue.start_consuming(__on_items_message__)