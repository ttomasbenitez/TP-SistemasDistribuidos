import socket
import logging
import signal
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from pkg.message.message import Message

class ConnectionClosedException(Exception):
    """Exception raised when a client connection is closed unexpectedly."""
    pass

class Gateway:
    def __init__(self, port, listen_backlog, exchange):
        """
        Initializes the gateway, binds the socket to the given port, and sets up shared resources and locks.
        """
        # Initialize gateway socket
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))
        self._gateway_socket.listen(listen_backlog)
        self._running = True
        self._client_socket = None
        self._exchange = exchange 
    
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        """
        Main gateway loop to accept new client connections and handle them in separate processes.
        Synchronizes all processes using a barrier.
        """        
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                self._client_socket = client_sock
                self.__receive_data()
            except OSError as e:
                if self._running:
                    logging.error(f'action: accept_connections | result: fail | error: {e}')
                else:
                    break

        self.__handle_shutdown(None, None)

    def __receive_data(self):
        all_received = False
        while not all_received:
            message = Message.read_message(self._client_socket)
            if message.type == MESSAGE_TYPE_EOF:
                all_received = True
            logging.info(f'action: receive_data | result: success | request_id: {message.request_id} | type: {message.type} | msg_num: {message.msg_num}')
            self._exchange.send(message.serialize(), str(message.type))
            logging.info(f'action: send message via exchange | result: success | type: {message.type}')
    
    def __accept_new_connection(self):
        """
        Accepts new client connections and returns the client socket.
        """
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._gateway_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
    def __handle_shutdown(self, signum, frame):
        """
        Closes all client connections and shuts down the gateway.
        """
        self._running = False
        self._client_socket.close()
        self._gateway_socket.close()
        logging.info(f'action: gateway shutdown | result: success')