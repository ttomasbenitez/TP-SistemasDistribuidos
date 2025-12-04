
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
  
    def __init_client_handler__(self, items_input_queue: str, host: str, expected_eofs: int, state_storage):
        
        self.__init_manager__()
        self.__init_middlewares_handler__()
        self.items_input_queue = items_input_queue
        self.connection = PikaConnection(host)
        self.expected_eofs = expected_eofs
        self.eofs_by_client = {}
        self._sender_lock = threading.Lock()
        self.state_storage = state_storage
        state_storage.load_state_all()
        
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
    
    def _sender_key(self, message: Message, stream: str) -> str:
        try:
            sender_id = message.get_node_id_and_request_id()
        except Exception:
            # Fallback to node_id only if combined is not available
            sender_id = "unknown_sender" if message.node_id is None else message.node_id
            sender_id = f"{sender_id}.{message.request_id}"
        return f"{stream}:{sender_id}"

    def is_dupped(self, message: Message, stream: str = "data") -> bool:
        """
        Returns True if the message is a duplicate or out-of-order for the given stream
        (based on last seen msg_num for its sender+request). Updates last seen on accept.
        """
        logging.info(f"el mensaje {message.type} tiene msg num {message.msg_num} y request id {message.request_id} y nodo id {message.node_id}")
        key = self._sender_key(message, stream)
        logging.info(f'key {key}')
        state = self.state_storage.get_data_from_request(message.request_id)
        last_msg_by_sender = state.get("last_by_sender", {})
        logging.info(f'last_msg_by_sender {last_msg_by_sender}')
        last = last_msg_by_sender.get(key, -1)
        logging.info(f'las {last}')
        if message.msg_num <= last:
            if message.msg_num == last:
                logging.info(f"action: duplicate_msg | stream:{stream} | sender:{key} | msg:{message.msg_num}")
            else:
                logging.warning(f"action: out_of_order_msg | stream:{stream} | sender:{key} | msg:{message.msg_num} < last:{last}")
            return True
        state["last_by_sender"][key] = message.msg_num
        self.state_storage.data_by_request[message.request_id] = state
        return False
    
    def _process_on_eof_message__(self, message):
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
        items_input_queue = MessageMiddlewareQueue(self.items_input_queue, self.connection)
        self.message_middlewares.append(items_input_queue)
        
        def __on_items_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
             
            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)
            
            # dedup/ordering for items stream
            logging.info(f"Checking duplication for items stream | request_id: {message.request_id} | msg_num: {message.msg_num}")
            if self.is_dupped(message, stream="items"):
                return
                 
            self._process_items_to_join(message)
            self.state_storage.save_state(message.request_id)
            self.state_storage.cleanup_data(message.request_id)
            
        items_input_queue.start_consuming(__on_items_message__)