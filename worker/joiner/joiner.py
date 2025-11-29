
from worker.base import Worker 
from abc import ABC, abstractmethod
import threading
from utils.heartbeat import start_heartbeat_sender
import logging
from pkg.message.message import Message
from Middleware.middleware import MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF

class Joiner(Worker, ABC):
  
    def __init_client_handler__(self, items_input_queue: str, host: str, expected_eofs: int):
        
        self.__init_middlewares_handler__()
        self.items_input_queue = items_input_queue
        self.host = host
        self.expected_eofs = expected_eofs
        self.eofs_by_client = {}
        self.items_to_join = {}
        
        self.items_to_join_lock = threading.Lock()
        self.eofs_lock = threading.Lock()
        
    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
        logging.info(f"Starting Joiner worker")
        t_data = threading.Thread(target=self._consume_data_queue)
        t_items_to_join = threading.Thread(target=self._consume_items_to_join_queue)
        t_data.start()
        t_items_to_join.start()
        t_data.join()
        t_items_to_join.join()

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
        with self.eofs_lock:
            self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            if self.eofs_by_client[message.request_id] < self.expected_eofs:
                logging.info(f"action: EOF message received {self.eofs_by_client[message.request_id]}/{self.expected_eofs} | request_id: {message.request_id} | type: {message.type}")
                return
        try:
            logging.info(f"action: EOF message received {self.eofs_by_client[message.request_id]}/{self.expected_eofs} send results | request_id: {message.request_id} | type: {message.type}")
            self._send_results(message)
        except Exception as e:
            logging.error(f"Error al procesar mensajes pendientes: {e}")
        return
    
    def _consume_items_to_join_queue(self):
        items_input_queue = MessageMiddlewareQueue(self.host, self.items_input_queue)
        self.message_middlewares.append(items_input_queue)
        def __on_items_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
             
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            
            self._process_items_to_join(message)
            
        items_input_queue.start_consuming(__on_items_message__)