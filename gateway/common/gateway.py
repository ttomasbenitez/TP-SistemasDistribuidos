import socket
import logging
import signal
import uuid
from .client_handler import ClientHandler
from Middleware.middleware import MessageMiddlewareQueue

class Gateway:
    def __init__(self, port, listen_backlog, exchange, in_queue_prefix, rabbitmq_host):
        self._listen_backlog = listen_backlog
        self._exchange = exchange
        self._in_queue_prefix = in_queue_prefix
        self._running = True
        self._clients = []
        self._rabbitmq_host = rabbitmq_host

        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))

        # Manejo de señales para apagar el gateway limpiamente
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        """Loop principal del Gateway: acepta clientes y lanza un hilo por cada uno."""

        self._gateway_socket.listen(self._listen_backlog)

        while self._running:
            try:
                client_sock, addr = self._gateway_socket.accept()
                logging.info(f"action: new_connection | ip: {addr[0]} | result: success")

                client_id = str(uuid.uuid4())
                results_queue_name = f"{self._in_queue_prefix}_{client_id}"
                results_in_queue = MessageMiddlewareQueue(self._rabbitmq_host, results_queue_name)

                # Crear un handler por cliente
                handler = ClientHandler(client_id, client_sock, self._exchange, results_in_queue)

                self._clients.append(handler)
                handler.start()  # arranca el hilo del cliente

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

        # Cerrar socket principal
        try:
            if self._gateway_socket:
                self._gateway_socket.close()
        except Exception:
            pass

        # Esperar a que todos los clientes terminen
        for handler in self._clients:
            try:
                handler.join(timeout=5)
            except Exception:
                pass

        logging.info("action: gateway_shutdown | result: success")
