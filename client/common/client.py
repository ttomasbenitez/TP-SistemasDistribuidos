#!/usr/bin/env python3

import socket
import logging
import signal
import os
from common.file_reader import FileReader
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS
from pkg.message.message import Message
from pkg.message.protocol import Protocol
from pkg.storage.result_storage import ResultStorage
from pathlib import Path

class Client:
    def __init__(self, gateway_host: str, gateway_port: int):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gateway_host = gateway_host
        self._gateway_port = gateway_port
        self._socket.bind(('', 0))
        self._protocol = None
    
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def run(self):
        try:
            self._socket.connect((self._gateway_host, self._gateway_port))
            self._protocol = Protocol(self._socket)
            logging.info(f'action: connect | result: success | gateway address: {self._gateway_host}:{self._gateway_port}')
            self.__send_request()
            self.__wait_for_results()
        except Exception as e:
            logging.error(f'action: connect | result: fail | error: {e}')
            
        self.__handle_shutdown(None, None)

    def __send_request(self):
        """
        Sends a request to the gateway.
        """
        try:
            self.__send_data()
            self.__send_end_of_data()
            logging.info(f'action: send_request | result: success')
        except Exception as e:
            logging.error(f'action: send_request | result: fail | error: {e}')

    def __send_data(self):
        self.__send_folder_data(os.getenv('STORES_FOLDER_PATH'), MESSAGE_TYPE_STORES)
        self.__send_folder_data(os.getenv('MENU_ITEMS_FOLDER_PATH'), MESSAGE_TYPE_MENU_ITEMS)
        self.__send_folder_data(os.getenv('USERS_FOLDER_PATH'), MESSAGE_TYPE_USERS)
        self.__send_folder_data(os.getenv('TRANSACTIONS_FOLDER_PATH'), MESSAGE_TYPE_TRANSACTIONS)
        self.__send_folder_data(os.getenv('TRANSACTION_ITEMS_FOLDER_PATH'), MESSAGE_TYPE_TRANSACTION_ITEMS)

    def __send_folder_data(self, folder_path, message_type):
        folder = Path(folder_path)
        for file_path in folder.glob("*.csv"):
            file_reader = FileReader(file_path, int(os.getenv('MAX_BATCH_SIZE')))
            while file_reader.has_more_data():
                data = file_reader.get_chunk()
                self._protocol.send_message(Message(0, message_type, 0, data).serialize())
            file_reader.close()

        logging.info(f'action: send_data_folder | folder: {folder_path} | result: success')

    def __send_end_of_data(self):
        """
        Sends an EOF message to the gateway to indicate the end of data transmission.
        """
        try:
            self._protocol.send_message(Message(0, MESSAGE_TYPE_EOF, 0, 0).serialize())
            logging.info(f'action: send_eof | result: success')
        except Exception as e:
            logging.error(f'action: send_eof | result: fail | error: {e}')

    def __handle_shutdown(self, signum, frame):
        """
        Closes gateway connection and shuts down the client.
        """
        self._protocol.close()
        logging.info(f'action: client shutdown | result: success')
        
    def __wait_for_results(self):
        results_storage = ResultStorage()
        message = self._protocol.read_message()
        logging.info(f'action: receive_message | result: success | message type: {message.type}')
        results_storage.start_run(message.request_id)
        while True:
            try:
                message = self._protocol.read_message()
                if not message:
                    break
                results_storage.add_chunk(message)
                logging.info(f'action: receive_message | result: success | message type: {message.type}')
            except Exception as e:
                logging.error(f'action: receive_message | result: fail | error: {e}')
                break