#!/usr/bin/env python3

import socket
import logging
import signal
from common.file_reader import FileReader
from pkg.message.message import Message

class Client:
    def __init__(self, gateway_host: str, gateway_port: int, file_reader: FileReader):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_host = gateway_host
        self._gateway_port = gateway_port
        self._socket.bind(('', 0))
        self._file_reader = file_reader
    
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        try:
            self._socket.connect((self._gateway_host, self._gateway_port))
            logging.info(f'action: connect | result: success | gateway address: {self._gateway_host}:{self._gateway_port}')
            data = self._file_reader.get_chunk()
            Message(0, 'FILE', 0, data).send_message(self._socket)
        except Exception as e:
            logging.error(f'action: connect | result: fail | error: {e}')

        self.__handle_shutdown(None, None)

    def __handle_shutdown(self, signum, frame):
        """
        Closes gateway connection and shuts down the client.
        """
    
        self._socket.close()
        logging.info(f'action: client shutdown | result: success')