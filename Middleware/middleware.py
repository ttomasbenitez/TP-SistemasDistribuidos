from abc import ABC, abstractmethod
import pika

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	@abstractmethod
	def stop_consuming(self):
		pass
	
	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def send(self, message):
		pass

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	@abstractmethod
	def close(self):
		pass

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	@abstractmethod
	def delete(self):
		pass

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, heartbeat=600))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.connection.close()

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        self.connection.close()

    def delete(self):
        self.channel.queue_delete(queue=self.queue_name)

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, queues_dict):
        self.exchange_name = exchange_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
        self.route_keys = list(queues_dict.values())

        # Diccionario que guarda cola -> {"queue": objeto Queue, "routing_key": routing_key}
        self.queues = {}

        for queue_name, routing_keys in queues_dict.items():
            # Crear la cola
            queue = MessageMiddlewareQueue(host, queue_name)
            # Bindearla al exchange con la routing key correspondiente
            # Bindear la cola al exchange por cada routing key si es lista
            if isinstance(routing_keys, list):
                for key in routing_keys:
                    self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=key)
            else:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_keys)

            self.queues[queue_name] = {"queue": queue, "routing_key": routing_keys}

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        self.connection.close()

    def send(self, message, routing_key):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

    def close(self):
        self.connection.close()

    def delete(self):
        self.channel.exchange_delete(exchange=self.exchange_name)