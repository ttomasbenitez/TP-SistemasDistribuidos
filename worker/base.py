from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware, MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF
from pkg.message.message import Message
import signal
import logging
from multiprocessing import Manager

class Worker(ABC):
    
    @abstractmethod
    def __init__(self):
        pass
        
    def __init_middlewares_handler__(self):
        self.message_middlewares = []
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        
    def __init_manager__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.inflight = self.manager.dict()
        self.drained = self.manager.dict()
        
    @abstractmethod
    def start(self):
        pass
    
    def __on_message__(self, raw):
        pass

    def _ensure_request(self, request_id):
        with self.lock:
            if request_id not in self.inflight:
                self.inflight[request_id] = 0
            if request_id not in self.drained:
                ev = self.manager.Event()
                ev.set()
                self.drained[request_id] = ev

    def _inc_inflight(self, request_id):
        with self.lock:
            self.inflight[request_id] += 1
            if self.inflight[request_id] > 0:
                self.drained[request_id].clear()

    def _dec_inflight(self, request_id):
        with self.lock:
            self.inflight[request_id] -= 1
            if self.inflight[request_id] <= 0:
                self.inflight[request_id] = 0
                self.drained[request_id].set()
                
    def _consume_eof(self):
        eof_service_queue = MessageMiddlewareQueue(self.eof_service_queue, self.connection)
        eof_self_queue = MessageMiddlewareQueue(self.eof_self_queue, self.connection)
        self.message_middlewares.extend([eof_service_queue, eof_self_queue])
        
        def on_eof_message(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en nodo | request_id: {message.request_id} | type: {message.type}")
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    eof_service_queue.send(message.serialize())
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        eof_self_queue.start_consuming(on_eof_message)
    
    def stop(self):
        try:
            for message_middleware in self.message_middlewares:
                message_middleware.stop_consuming()
            if hasattr(self, 'heartbeat_sender') and self.heartbeat_sender:
                self.heartbeat_sender.stop()
        except Exception as e:
            print(f"Error al detener: {type(e).__name__}: {e}")
         
    def close(self):
        try:
            for message_middleware in self.message_middlewares:
                message_middleware.close()
            
        except Exception as e:
            print(f"Error al detener: {type(e).__name__}: {e}")
    
    def __handle_shutdown(self, signum, frame):
        """
        Closes all worker connections and shuts down the worker.
        """ 
        try:
            self.stop()
        except Exception:
            logging.error(f'action: worker shutdown | result: failed to stop consuming | signal: {signum}')  
        try:
            self.close()
        except Exception:
            logging.error(f'action: worker shutdown | result: failed to close | signal: {signum}')  
        logging.info(f'action: worker shutdown | result: success')
        
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
        key = self._sender_key(message, stream)
        state = self.state_storage.get_data_from_request(message.request_id)
        last_msg_by_sender = state.get("last_by_sender", {})
        last = last_msg_by_sender.get(key, -1)
        if message.msg_num <= last:
            if message.msg_num == last:
                logging.info(f"action: duplicate_msg | stream:{stream} | sender:{key} | msg:{message.msg_num}")
            else:
                logging.warning(f"action: out_of_order_msg | stream:{stream} | sender:{key} | msg:{message.msg_num} < last:{last}")
            return True
        state["last_by_sender"][key] = message.msg_num
        self.state_storage.data_by_request[message.request_id] = state
        return False
    
        