from abc import ABC, abstractmethod
import logging
import pika
from Middleware.connection import PikaConnection

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
    def __init__(self, queue_name: str, connection: PikaConnection):
        self.queue_name = queue_name
        self.connection = connection
        self._connect_to_channel()
        
    def _connect_to_channel(self):
        self.connection.declare_queue(self.queue_name)

    def start_consuming(self, on_message_callback, init_consuming=True, manual_ack=False, prefetch_count=2):
        def callback(ch, method, properties, body):
            if manual_ack:
                on_message_callback(body, ch, method)
            else:
                try:
                    on_message_callback(body)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logging.error(f"[Middleware] Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    
        self.connection.add_basic_consume(
            queue_name=self.queue_name,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            auto_ack=False
        )

    def start_consuming_with_batch_ack(self, on_message_callback, prefetch_count=100):
        """
        Consume messages with manual ACK for batch processing.
        Optimized prefetch count for batch operations (default 100).
        Callback signature: on_message_callback(body, ch, method)
        """
        def callback(ch, method, properties, body):
            on_message_callback(body, ch, method)
                    
        self.connection.add_basic_consume(
            queue_name=self.queue_name,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            auto_ack=False
        )

    def stop_consuming(self):
        self.connection.stop_consuming()
        
    def send(self, message):
        try:
            self.connection.send(exchange='', routing_key=self.queue_name, body=message)
        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                pika.exceptions.ChannelClosedByBroker) as e:
            logging.warning(f"[AMQP] Conexión perdida ({type(e).__name__}). Reintentando...")
            try:
                self.reconnect()
                self.connection.send(exchange='', routing_key=self.queue_name, body=message)
                logging.info("[AMQP] Reenvío exitoso tras reconexión.")
            except Exception as e2:
                logging.error(f"[AMQP] Falló reintento tras reconexión: {type(e2).__name__}: {e2}")

    def close(self):
        self.connection.stop_consuming()

    def delete(self):
        self.connection.delete_queue(queue=self.queue_name)

    def reconnect(self):
        try:
            self.close()
        except Exception as e:
            logging.warning(f"Error cerrando conexión vieja: {e}")
        logging.info("Reconectando con RabbitMQ...")
        self.connection.reconnect()
        self._connect_to_channel()

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, exchange_name: str, queues_dict: object, connection: PikaConnection):
        self.exchange_name = exchange_name
        self.exchange_queues = queues_dict
        self.connection = connection
        self._connect_to_channel()
        
    def _connect_to_channel(self):
        self.connection.declare_exchange(self.exchange_name, exchange_type='topic')
        self.route_keys = list(self.exchange_queues.values())
        # Diccionario que guarda cola -> {"queue": objeto Queue, "routing_key": routing_key}
        self.queues = {}

        for queue_name, routing_keys in self.exchange_queues.items():
            # Crear la cola
            queue = MessageMiddlewareQueue(queue_name, connection=self.connection)
            # Bindearla al exchange con la routing key correspondiente
            # Bindear la cola al exchange por cada routing key si es lista
            if isinstance(routing_keys, list):
                for key in routing_keys:
                    self.connection.bind_queue(queue_name=queue_name, exchange=self.exchange_name, routing_key=key)
            else:
                self.connection.bind_queue(queue_name=queue_name, exchange=self.exchange_name, routing_key=key)

            self.queues[queue_name] = {"queue": queue, "routing_key": routing_keys}

        
    def start_consuming(self, on_message_callback, init_consuming=True):
        def callback(ch, method, properties, body):
            on_message_callback(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        for queue_name in self.queues.keys():
            self.connection.add_basic_consume(
                queue_name=queue_name,
                on_message_callback=callback
            )

    def stop_consuming(self):
        self.connection.stop_consuming()

    def send(self, message, routing_key):
        try:
            self.connection.send(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message
            )
        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                pika.exceptions.ChannelClosedByBroker) as e:
            logging.warning(f"[AMQP] Conexión perdida ({type(e).__name__}). Reintentando...")
            try:
                self.connection.reconnect()
                self.connection.send(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message
                )
                logging.info("[AMQP] Reenvío exitoso tras reconexión.")
            except Exception as e2:
                logging.error(f"[AMQP] Falló reintento tras reconexión: {type(e2).__name__}: {e2}")

    def close(self):
        self.connection.close()

    def delete(self):
        self.connection.delete_exchange(exchange=self.exchange_name)
    
    def reconnect(self):
        try:
            self.close()
        except Exception as e:
            logging.warning(f"Error cerrando conexión vieja: {e}")
        logging.info("Reconectando con RabbitMQ...")
        self.connection.reconnect()
        self._connect_to_channel()
        
