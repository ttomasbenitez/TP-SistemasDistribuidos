from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddleware
import logging
import signal
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF
from abc import ABC, abstractmethod
from utils.heartbeat import start_heartbeat_sender
from Middleware.connection import PikaConnection
class EofService(Worker, ABC):
  
    def __init__(self, eof_input_queque: str, eof_output_middleware: str, expected_acks: int, host: str):
        self.connection = PikaConnection(host)
        self.eof_input_queque = eof_input_queque
        self.eof_output_middleware = eof_output_middleware
        self.expected_acks = expected_acks
        self.acks_by_client = dict()
        
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        
    def start(self):
        
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_eof_queue()
        logging.info("Eof Service iniciado y consumiendo mensajes.")
        self.connection.start_consuming()
      
    def _consume_eof_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.eof_input_queque, self.connection)
        
        def __on_message__(message):
            try:
                message = Message.deserialize(message)
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en service queue | request_id: {message.request_id}")
                    self.acks_by_client[message.request_id] = self.acks_by_client.get(message.request_id, 0) + 1  
                    if self.acks_by_client[message.request_id] == self.expected_acks:
                        logging.info(f"Enviando final EOF del cliente {message.request_id}")
                        self.send_message(message)
                        del self.acks_by_client[message.request_id]
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
                
        data_input_queue.start_consuming(__on_message__)
    
    @abstractmethod         
    def send_message(self, message):
        pass
     
    #TODO           
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
        
