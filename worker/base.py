from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware, MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF
from pkg.message.message import Message
import signal
import logging
from multiprocessing import Manager
from pkg.storage.state_storage.eof_storage import EofStorage

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
        
        def _on_eof_message(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en nodo | request_id: {message.request_id} | type: {message.type}")
                    self._ensure_request(message.request_id)
                    self.drained[message.request_id].wait()
                    eof_message = Message(message.request_id, MESSAGE_TYPE_EOF, message.msg_num, '', self.node_id)
                    eof_service_queue.send(eof_message.serialize())
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        eof_self_queue.start_consuming(_on_eof_message)
    
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
        
    def on_eof_message(self, message: Message, dedup_strategy, eof_storage: EofStorage, expected_eofs: int):
        if dedup_strategy.is_duplicate(message):
            logging.info(f"Mensaje EOF duplicado ignorado | request_id: {message.request_id}")
            return
        
        logging.info(f"EOF recibido | request_id: {message.request_id}")
        
        state = eof_storage.get_state(message.request_id)
        state["eofs_count"] = state.get('eofs_count', 0) + 1  
        if state["eofs_count"] == expected_eofs:
            return True
        else:
            eof_storage.data_by_request[message.request_id] = state
            eof_storage.save_state(message.request_id)
            return False