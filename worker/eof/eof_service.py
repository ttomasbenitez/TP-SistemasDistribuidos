from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddleware
import logging
import signal
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF
from abc import ABC, abstractmethod

class EofService(Worker, ABC):
  
    def __init__(self, expected_acks: int, eof_in_queque: MessageMiddlewareQueue, eof_out_middleware: MessageMiddleware):
        super().__init__(eof_in_queque)
        self.eof_out_middleware = eof_out_middleware
        self.expected_acks = expected_acks
        self.acks_by_client = dict()
        
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)
      
    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"EOF recibido en service queue | request_id: {message.request_id}")
                self.acks_by_client[message.request_id] = self.acks_by_client.get(message.request_id, 0) + 1  
                if self.acks_by_client[message.request_id] == self.expected_acks:
                    logging.info(f"Enviando final EOF del cliente {message.request_id}")
                    self.send_message_to_output(message)
                    del self.acks_by_client[message.request_id]
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
    
    def send_message(self, message):
        try:
            self.in_middleware.send(message)
        except Exception as e:
            print(f"Error al enviar el mensaje: {type(e).__name__}: {e}")
    
    @abstractmethod         
    def send_message_to_output(self, message):
        pass
                
    def close(self):
        try:
            self.in_middleware.close()
            self.eof_out_middleware.close()
        except Exception as e:
            print(f"Error al cerrar: {type(e).__name__}: {e}")
            
    def __handle_shutdown(self, signum, frame):
        """
        Closes all worker connections and shuts down the worker.
        """ 
        try:
            self.close()
        except Exception:
            pass
        logging.info(f'action: gateway shutdown | result: success')
        
