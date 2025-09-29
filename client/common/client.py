#!/usr/bin/env python3

import socket
import logging
import signal
import os
from common.file_reader import FileReader
from packages.messages.message import Message
from packages.messages.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES

class Client:
    def __init__(self, gateway_host: str, gateway_port: int):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_host = gateway_host
        self._gateway_port = gateway_port
        self._socket.bind(('', 0))
    
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        try:
            self._socket.connect((self._gateway_host, self._gateway_port))
            logging.info(f'action: connect | result: success | gateway address: {self._gateway_host}:{self._gateway_port}')
            self.__send_request()
        except Exception as e:
            logging.error(f'action: connect | result: fail | error: {e}')

        self.__handle_shutdown(None, None)


    def __send_request(self):
        """
        Sends a request to the gateway.
        """
        try:
            self.__send_stores_data()
            #self.__send_menu_items_data()
            logging.info(f'action: send_request | result: success')
        except Exception as e:
            logging.error(f'action: send_request | result: fail | error: {e}')

    def __send_stores_data(self):
        file_reader = FileReader(os.getenv('STORES_FILE_PATH'), int(os.getenv('MAX_BATCH_SIZE')))
        while file_reader.has_more_data():
            data = file_reader.get_chunk()
            Message(0, MESSAGE_TYPE_STORES, 0, data).send_message(self._socket)
        file_reader.close()
        Message(0, MESSAGE_TYPE_EOF, 0, 0).send_message(self._socket)
        logging.info(f'action: send_stores_data | result: success')

    def __send_menu_items_data(self):
        file_reader = FileReader(os.getenv('MENU_ITEMS_FILE_PATH'), int(os.getenv('MAX_BATCH_SIZE')))
        while file_reader.has_more_data():
            data = file_reader.get_chunk()
            Message(0, MESSAGE_TYPE_MENU_ITEMS  , 0, data).send_message(self._socket)
        file_reader.close()
        Message(0, MESSAGE_TYPE_EOF, 0, 0).send_message(self._socket)
        logging.info(f'action: send_menu_items_data | result: success')

    def __handle_shutdown(self, signum, frame):
        """
        Closes gateway connection and shuts down the client.
        """
    
        self._socket.close()
        logging.info(f'action: client shutdown | result: success')