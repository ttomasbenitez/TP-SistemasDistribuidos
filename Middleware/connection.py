
import pika

class PikaConnection():
    
    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None
        
    def connect(self): 
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, heartbeat=600))
        
    def open_channel(self):
        if self.connection is None or self.connection.is_closed:
            self.connect()
        self.channel = self.connection.channel()
        try:
            # Enable publisher confirms for reliability
            self.channel.confirm_delivery()
        except Exception:
            pass
        
    def start(self):
        self.connect()
        self.open_channel()
        
    def add_basic_consume(self, queue_name: str, on_message_callback, prefetch_count: int = 2, auto_ack: bool =False):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
            
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=auto_ack)
        
    def start_consuming(self):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        self.channel.start_consuming()
        
    def send(self, exchange: str, routing_key: str, body: str, mandatory: bool = True):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, mandatory=mandatory)
        
    def declare_queue(self, queue_name: str):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.queue_declare(queue=queue_name, durable=True)
        
    def declare_exchange(self, exchange_name: str, exchange_type: str = 'topic'):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        
    def stop_consuming(self):
        if self.connection is not None and not self.connection.is_closed:
            self.connection.close() 
            
    def close(self):
        try:
            if self.channel is not None and getattr(self.channel, "is_open", False):
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection is not None and not self.connection.is_closed:
                self.connection.close()
        except Exception:
            pass
            
    def delete_queue(self, queue_name: str):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.queue_delete(queue=queue_name)
        
    def delete_exchange(self, exchange_name: str):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.exchange_delete(exchange=exchange_name)
        
    def bind_queue(self, queue_name: str, exchange: str, routing_key: str):
        if self.channel is None or self.channel.is_closed:
            self.open_channel()
        
        self.channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)
        
    def reconnect(self):
        self.stop_consuming()
        self.connect()
        self.open_channel()
