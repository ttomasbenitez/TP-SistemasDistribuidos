import socket
import logging
import signal
import threading
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_REQUEST_ID, MESSAGE_TYPE_CLIENT_ID
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from pkg.message.protocol import Protocol

EXPECTED_QUERIES = 4
class ConnectionClosedException(Exception):
    """Exception raised when a client connection is closed unexpectedly."""
    pass

class Gateway:
    def __init__(self, port, listen_backlog, exchange: MessageMiddlewareExchange, in_queue: MessageMiddlewareQueue):
        """
        Initializes the gateway, binds the socket to the given port, and sets up shared resources and locks.
        """
        
        self._gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_socket.bind(('', port))
        self._gateway_socket.listen(listen_backlog)
        self._running = True
        self._client_protocol = None
        self._exchange = exchange 
        self._in_queue = in_queue
        self._consumer_thread: threading.Thread = None
        self._results_started = False
        self._finished_queries = 0
        self._client_id = 0 # Identificador único autoincremental para cada cliente
    
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
                logging.info(f'action: new_connection | result: success | client_id: {self._client_id}')
                self._client_protocol = Protocol(client_sock)

                self._send_id()
                self.__receive_data()
                self.__start_results_consumer_once()    
            except OSError as e:
                if self._running:
                    logging.error(f'action: accept_connections | result: fail | error: {e}')
                else:
                    break

        self.__handle_shutdown(None, None)

    def __receive_data(self):
        all_received = False
        while not all_received:
            message = self._client_protocol.read_message()
            if message.type == MESSAGE_TYPE_EOF:
                all_received = True
            else:
                items = message.process_message_from_csv()
                dropped_columns_chunks = ''
                for item in items:
                    dropped_columns_chunks += item.serialize()
                message.update_content(dropped_columns_chunks)
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
    
    def __start_results_consumer_once(self):
        """
        Levanta un hilo que llama start_consuming() (bloqueante) en su propia conexión.
        Se llama una sola vez por conexión/solicitud.
        """
        if self._results_started:
            return

        self._results_started = True
        self._client_protocol.send_message(Message(0, MESSAGE_TYPE_REQUEST_ID, 0, '').serialize()) #ESTO ES UNA ESPECIA DE ACK PARA EL CLIENTE

        def _consume():
            try:
                self._in_queue.start_consuming(self.__on_result_message)
            except Exception as e:
                logging.error(f"action: consume_messages | result: fail | error: {e}")

        self._consumer_thread = threading.Thread(target=_consume, daemon=True)
        self._consumer_thread.start()
    
    def __on_result_message(self, message):
        proceced_message = Message.deserialize(message)
        if proceced_message.type == MESSAGE_TYPE_EOF:
            self._finished_queries += 1
            if self._finished_queries == EXPECTED_QUERIES:
                logging.info(f'action: all results sent | result: success')
            return
        self._client_protocol.send_message(message)
        
    def __handle_shutdown(self, signum, frame):
        """
        Closes all client connections and shuts down the gateway.
        """
        self._running = False
       
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=2)

        try:
            if self._client_protocol:
                self._client_protocol.close()
        except Exception:
            pass

        try:
            self._gateway_socket.close()
        except Exception:
            pass
        logging.info(f'action: gateway shutdown | result: success')
        
    def _send_id(self):
        """
        Sends the client ID to the connected client.
        """
        self._client_protocol.send_message(Message(0, MESSAGE_TYPE_CLIENT_ID, 0, str(self._client_id)).serialize())
        self._client_id += 1