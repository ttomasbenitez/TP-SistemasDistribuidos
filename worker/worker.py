from abc import ABC, abstractmethod
from Middleware.middleware import MessageMiddleware

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
    
    @abstractmethod  
    def stop(self):
        self._running = False
        try:
            self.in_middleware.stop_consuming()
        except Exception as e:
            print(f"Error al detener: {type(e).__name__}: {e}")
            
    @abstractmethod      
    def close(self):
        pass