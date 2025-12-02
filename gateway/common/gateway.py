import socket
import logging
import signal
import uuid
from Middleware.connection import PikaConnection
from .client_handler import ClientHandler
from Middleware.middleware import MessageMiddlewareQueue
from Middleware.middleware import MessageMiddlewareExchange
from pkg.message.constants import MESSAGE_TYPE_USERS, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
import os

class Gateway:
    def __init__(self, port, listen_backlog, exchange_name, in_queue_prefix, output_exchange_name,  rabbitmq_host):
        self._listen_backlog = listen_backlog
        self._exchange_name = exchange_name
        self._in_queue_prefix = in_queue_prefix
        self._running = True
        self._clients = []
        self._rabbitmq_host = rabbitmq_host
        self._request_id = 0
        self._output_exchange_name = output_exchange_name

        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))

        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        """Loop principal del Gateway: acepta clientes y lanza un hilo por cada uno."""

        self._gateway_socket.listen(self._listen_backlog)

        while self._running:
            try:
                client_sock, addr = self._gateway_socket.accept()
                logging.info(f"action: new_connection | ip: {addr[0]} | result: success")

                results_queue_name = f"{self._in_queue_prefix}_{self._request_id}"
                connection = PikaConnection(self._rabbitmq_host)
                results_in_queue = MessageMiddlewareQueue(results_queue_name, connection)
                # results_in_queue.channel.exchange_declare(exchange=self._output_exchange_name, exchange_type='topic', durable=True)
                # results_in_queue.bind_queue(
                #     self._output_exchange_name,
                #      str(self._request_id),
                # )
               # logging.info(f"action: bind_results_queue | queue name: {results_queue_name} | exchange: {self._output_exchange_name} | result: success")

                queues_dict = self.create_queues_dict()
                exchange = MessageMiddlewareExchange(self._exchange_name, queues_dict, connection)

                handler = ClientHandler(self._request_id, client_sock, exchange, results_in_queue, self._output_exchange_name, connection)
                self._request_id += 1

                self._clients.append(handler)
                handler.start()

            except OSError as e:
                if self._running:
                    logging.error(f"action: accept_connection | result: fail | error: {e}")
                break

        self.__handle_shutdown(None, None)

    def __handle_shutdown(self, signum, frame):
        """Apaga el Gateway y espera que terminen todos los clientes."""
        if not self._running:
            return

        logging.info("action: gateway_shutdown | result: in_progress")
        self._running = False

        try:
            if self._gateway_socket:
                self._gateway_socket.close()
        except Exception:
            pass

        for handler in self._clients:
            try:
                handler.join(timeout=5)
            except Exception:
                pass

        logging.info("action: gateway_shutdown | result: success")

    def create_queues_dict(self):
        queues_dict = {}

        for key, queue_name in os.environ.items():
            if key.startswith("OUTPUT_QUEUE_"):
                if queue_name.startswith("users"):
                    routing_key = [str(MESSAGE_TYPE_USERS), str(MESSAGE_TYPE_EOF)]
                elif queue_name.startswith("menu"):
                    routing_key = [str(MESSAGE_TYPE_MENU_ITEMS), str(MESSAGE_TYPE_EOF)]
                elif queue_name.startswith("stores"):
                    routing_key = [str(MESSAGE_TYPE_STORES), str(MESSAGE_TYPE_EOF)]
                else:
                    routing_key = [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_TRANSACTION_ITEMS), str(MESSAGE_TYPE_EOF)]
                
                queues_dict[queue_name] = routing_key
        return queues_dict