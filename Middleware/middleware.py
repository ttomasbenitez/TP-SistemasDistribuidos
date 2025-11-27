from abc import ABC, abstractmethod
import logging
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
        self.host = host
        self._connect(host, queue_name)
        
    def _connect(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, heartbeat=6000))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def start_consuming(self, on_message_callback, manual_ack=False, prefetch_count=2):
        def callback(ch, method, properties, body):
            if manual_ack:
                on_message_callback(body, ch, method)
            else:
                on_message_callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            
        self.channel.start_consuming()

    def stop_consuming(self):
        self.connection.close()
        
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                pika.exceptions.ChannelClosedByBroker) as e:
            logging.warning(f"[AMQP] Conexión perdida ({type(e).__name__}). Reintentando...")
            try:
                self.reconnect()
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message,
                    mandatory=True
                )
                logging.info("[AMQP] Reenvío exitoso tras reconexión.")
            except Exception as e2:
                logging.error(f"[AMQP] Falló reintento tras reconexión: {type(e2).__name__}: {e2}")

    def close(self):
        self.connection.close()

    def delete(self):
        self.channel.queue_delete(queue=self.queue_name)
    
    def bind_queue(self, exchange_name, routing_key):
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=routing_key)
        
    def reconnect(self):
        try:
            self.close()
        except Exception as e:
            logging.warning(f"Error cerrando conexión vieja: {e}")
        logging.info("Reconectando con RabbitMQ...")
        self._connect(self.host, self.queue_name)

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, queues_dict):
        self.host = host
        self.exchange_name = exchange_name
        self.exchange_queues = queues_dict
        self._connect(host, exchange_name, queues_dict)
        
    def _connect(self, host, exchange_name, queues_dict):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, heartbeat=6000))
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
        def callback(ch, method, properties, body):
            on_message_callback(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        for queue_name in self.queues.keys():
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        self.channel.start_consuming()

    def stop_consuming(self):
        self.connection.close()

    def send(self, message, routing_key):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                mandatory=True
            )
        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                pika.exceptions.ChannelClosedByBroker) as e:
            logging.warning(f"[AMQP] Conexión perdida ({type(e).__name__}). Reintentando...")
            try:
                self.reconnect()
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    mandatory=True
                )
                logging.info("[AMQP] Reenvío exitoso tras reconexión.")
            except Exception as e2:
                logging.error(f"[AMQP] Falló reintento tras reconexión: {type(e2).__name__}: {e2}")

    def close(self):
        self.connection.close()

    def delete(self):
        self.channel.exchange_delete(exchange=self.exchange_name)
    
    def reconnect(self):
        try:
            self.close()
        except Exception as e:
            logging.warning(f"Error cerrando conexión vieja: {e}")
        logging.info("Reconectando con RabbitMQ...")
        self._connect(self.host, self.exchange_name, self.exchange_queues)
        
