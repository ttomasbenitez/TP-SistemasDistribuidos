import threading
import logging
from pkg.message.protocol import Protocol
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_REQUEST_ID
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from Middleware.connection import PikaConnection

EXPECTED_QUERIES = 4

class ClientHandler(threading.Thread):
    def __init__(self, request_id, client_sock, exchange: MessageMiddlewareExchange, in_queue: MessageMiddlewareQueue, output_exchange_name: str, connection: PikaConnection):
        super().__init__()
        self._request_id = request_id
        self._protocol = Protocol(client_sock)
        self._exchange = exchange
        self._in_queue = in_queue
        self._consumer_thread = None
        self._finished_queries = 0
        self._running = True
        self._output_exchange_name = output_exchange_name
        self.connection = connection

    def run(self):
        try:
            logging.info(f"action: client_handler_start | result: success | queue name: {self._in_queue.queue_name}")
            self._protocol.send_message(Message(self._request_id, MESSAGE_TYPE_REQUEST_ID, 0, '').serialize())
            self.connection.start()
            self.connection.declare_exchange(self._output_exchange_name, 'topic')
            self.connection.bind_queue(self._in_queue.queue_name, self._output_exchange_name, str(self._request_id))
          
            logging.info(f"action: bind_results_queue | queue name: {self._in_queue.queue_name} | exchange: {self._output_exchange_name} | result: success")

            self._receive_and_publish()
            self._start_results_consumer()
        except Exception as e:
            logging.error(f"action: client_handler_run | result: fail | error: {e}")
        finally:
            self.close()

    def _receive_and_publish(self):
        """Recibe los mensajes del cliente y los envía al exchange."""
        while True:
            message = self._protocol.read_message()
            if message.type == MESSAGE_TYPE_EOF:
                logging.info("action: client_handler receive_data | result: eof")
                self._exchange.send(message.serialize(), str(message.type))
                break

            items = message.process_message_from_csv()
            serialized = ''.join(i.serialize() for i in items)
            message.update_content(serialized)

            self._exchange.send(message.serialize(), str(message.type))
            logging.info(f"action: send_to_exchange | result: success | type: {message.type}")

    def _start_results_consumer(self):
    
        logging.info(f"action: start_results_consumer | result: in_progress from queue {self._in_queue.queue_name}")

        try:
            self._in_queue.start_consuming(self._on_result_message)
            self.connection.start_consuming()
        except Exception as e:
            logging.error(f"action: consume_results | result: fail | error: {e}")

    def _on_result_message(self, raw_msg):
        """Callback que envía los mensajes del backend al cliente."""
        msg = Message.deserialize(raw_msg)
        if msg.type == MESSAGE_TYPE_EOF:
            self._finished_queries += 1
            logging.info(f"action: send_eof_to_client | result: success | request_id: {msg.request_id} | finished_queries: {self._finished_queries}/{EXPECTED_QUERIES}")
            if self._finished_queries == EXPECTED_QUERIES:
                logging.info("action: all_results_sent | result: success")
                self._protocol.send_message(raw_msg)
            return
        self._protocol.send_message(raw_msg)

    def close(self):
        """Cierra ordenadamente protocolo, cola y socket."""
        logging.info("action: client_handler_close | result: in_progress")
        self._running = False
        try:
            if self.connection:
                self.connection.stop_consuming()
            if self._in_queue:
                self._in_queue.close()
            if self._exchange:
                self._exchange.close()
        except Exception:
            pass
        try:
            self._protocol.close()
        except Exception:
            pass
        logging.info("action: client_handler_close | result: success")
