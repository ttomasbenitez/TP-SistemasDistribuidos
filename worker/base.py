from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware, MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF
from pkg.message.message import Message
import signal
import logging
from multiprocessing import Manager
from utils.heartbeat import start_heartbeat_sender

class Worker(ABC):
    
    def __init__(self, in_middleware: MessageMiddleware, host: str = '', eof_self_queue: str = '', eof_service_queue: str = ''):
        self.in_middleware = in_middleware
        self.host = host
        self.eof_self_queue = eof_self_queue
        self.eof_service_queue = eof_service_queue
        self.__init_manager__()
        self.__init_middlewares_handler__()
        
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        
    def __init_middlewares_handler__(self):
        self.message_middlewares = []
        
    def __init_manager__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.inflight = self.manager.dict()
        self.drained = self.manager.dict()
        
    def start(self):
        
        self.heartbeat_sender = start_heartbeat_sender()
       
        try:
            self.in_middleware.start_consuming(self.__on_message__)
        except Exception as e:
            print(f"Error al consumir: {type(e).__name__}: {e}")

        self.stop()
        if hasattr(self, 'heartbeat_sender') and self.heartbeat_sender:
            self.heartbeat_sender.stop()
        self.close()
    
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
        eof_service_queue = MessageMiddlewareQueue(self.host, self.eof_service_queue)
        eof_self_queue = MessageMiddlewareQueue(self.host, self.eof_self_queue)
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
           
    def _send_groups(self, original_message: Message, groups: dict, out_middleware: MessageMiddleware):
        for key, items in groups.items():
            new_chunk = ''.join(item.serialize() for item in items)
            new_message = original_message.new_from_original(new_chunk)
            serialized = new_message.serialize()
            out_middleware.send(serialized)
         
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
            pass  
        try:
            self.close()
        except Exception:
            pass
        logging.info(f'action: gateway shutdown | result: success')
        