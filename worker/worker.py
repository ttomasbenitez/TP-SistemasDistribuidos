from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware
from pkg.message.message import Message
import logging

class Worker(ABC):
    
    def __init__(self, in_middleware: MessageMiddleware):
        self._running = False
        self.in_middleware = in_middleware
        
    def start(self):
        self._running = True
        
        while self._running:
            try:
                self.in_middleware.start_consuming(self.__on_message__)
            except Exception as e:
                print(f"Error al consumir: {type(e).__name__}: {e}")

        self.stop()
        self.close()
        
    @abstractmethod
    def __on_message__(self, raw):
        pass
    
    def stop(self):
        self._running = False
        try:
            self.in_middleware.stop_consuming()
        except Exception as e:
            print(f"Error al detener: {type(e).__name__}: {e}")
           
    def _send_groups(self, original_message: Message, groups: dict, out_middleware: MessageMiddleware):
        for key, items in groups.items():
            new_chunk = ''.join(item.serialize() for item in items)
            new_message = original_message.new_from_original(new_chunk)
            serialized = new_message.serialize()
            out_middleware.send(serialized)
            
    @abstractmethod      
    def close(self):
        pass