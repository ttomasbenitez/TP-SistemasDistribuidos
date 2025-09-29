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

    def __receive_request(self):
        message = Message.read_message(self._client_socket)
        logging.info(f'action: message_received | result: success')
        logging.info(f'action: process_request | result: success | request_id: {message.request_id} | type: {message.type} | msg_num: {message.msg_num}')
        logging.info(f'action: message_content | content: {message.content}')

    def __receive_data(self):
        all_received = False
        while not all_received:
            message = Message.read_message(self._client_socket)
            if message.type == MESSAGE_TYPE_EOF:
                all_received = True
            logging.info(f'action: message_received | result: success')
            logging.info(f'action: process_request | result: success | request_id: {message.request_id} | type: {message.type} | msg_num: {message.msg_num}')
            logging.info(f'action: message_content | content: {message.content}')
            self._exchange.send(message.serialize(), 'data')
            logging.info(f'action: send message via exchange')
    
    #def __receive_bet_data(self, sock):
    #    """
    #    Receives bet data from the client. Returns a list of Bet objects or None if no data is received.
    #    Ensures no short reads by receiving the entire message.
    #    """
    #    msg_type = self.__recv_all(sock, MESS_TYPE_BYTES)
    #    if msg_type == END_MESSAGE_TYPE:
    #        logging.debug(f'action: END_MESS_RECEIVED | result: success')   
    #        return None
    #    
    #    header = self.__recv_all(sock, MESS_LENGTH_BYTES)
    #    message_length = int.from_bytes(header[0:], "big")
    #    if message_length == 0:
    #        return None
#
    #    message = self.__recv_all(sock, message_length)
    #    return self.__parse_bet_data(message)

    #def __send_winner_to_client(self, client_sock, winner_bet):
    #    """
    #    Sends a single winner bet to the client.
    #    Uses sendall to handle short writes automatically.
    #    """
    #    message = DATA_MESSAGE_TYPE + f"{winner_bet.document}\n".encode('utf-8')
    #    client_sock.sendall(message)

    #def __send_end_message(self, client_sock):
    #    """
    #    Send end message to client.
    #    """
    #    client_sock.sendall(END_MESSAGE_TYPE)

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