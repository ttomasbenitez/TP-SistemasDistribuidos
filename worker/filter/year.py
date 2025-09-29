from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message

class FilterYearNode(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue, out_exchange: MessageMiddlewareExchange, years_set):
        super().__init__(in_queue)
        self.out_queue = out_queue
        self.out_exchange = out_exchange
        self.years = years_set
        
    def __on_message__(self, message):
        try:
            logging.info("Procesando mensaje")
            logging.info(f"{message}")
            message = Message.__deserialize__(message)
            items = message.process_message()
            logging.info("Mensaje procesado")
            logging.info(f"{items}")
            new_chunk = b''
            for item in items:
                year = item.get_year()
                if year in self.years:
                    new_chunk += item.serialize()
            if new_chunk:
                new_message = message.update_content(new_chunk)
                serialized = new_message.serialize()
                #self.out_queue.send(serialized)
                #self.out_exchange.send(serialized)
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
            
    def close(self):
        try:
            self.in_middleware.close()
            self.out_queue.close()
            self.out_exchange.close()
        except Exception as e:
            print(f"Error al cerrar: {type(e).__name__}: {e}")