from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware, MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_EOF
from pkg.message.message import Message
import signal
import logging
from multiprocessing import Manager
from pkg.message.utils import calculate_sub_message_id
from pkg.message.constants import SUB_MESSAGE_START_ID

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
        
        eof_self_queue.start_consuming(on_eof_message, False)
    
    def stop(self):
        try:
            for message_middleware in self.message_middlewares:
                message_middleware.stop_consuming()
            if hasattr(self, 'heartbeat_sender') and self.heartbeat_sender:
                self.heartbeat_sender.stop()
        except Exception as e:
            print(f"Error al detener: {type(e).__name__}: {e}")
           
    def _send_groups(self, original_message: Message, groups: dict, out_middleware: MessageMiddleware):
        sub_msg_id = SUB_MESSAGE_START_ID
        for key, items in groups.items():
            new_chunk = ''.join(item.serialize() for item in items)
            new_msg_num = calculate_sub_message_id(original_message.msg_num, sub_msg_id)
            new_message = original_message.new_from_original(new_chunk, msg_num=new_msg_num)
            serialized = new_message.serialize()
            out_middleware.send(serialized)
            sub_msg_id += 1
         
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
        