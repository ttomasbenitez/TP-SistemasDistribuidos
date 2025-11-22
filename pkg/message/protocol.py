import socket

from pkg.message.constants import (
    MESSAGE_SIZE_BYTES,
)     
from pkg.message.message import Message

class Protocol:
    def __init__(self, client_socket: socket):
        self._client_socket = client_socket
  
    def read_message(self):
        """
        Lee un mensaje desde el socket y lo deserializa. Evita short reads.
        :param socket: Socket desde el cual se lee el mensaje.
        """
        
        raw_msg_length = b''
        while len(raw_msg_length) < MESSAGE_SIZE_BYTES:
            chunk = self._client_socket.recv(MESSAGE_SIZE_BYTES - len(raw_msg_length))
            if chunk == b'':
                raise RuntimeError("Socket connection broken")
            raw_msg_length += chunk

        msg_length = int.from_bytes(raw_msg_length, byteorder='big')
        raw_msg = b''
        while len(raw_msg) < msg_length:
            chunk = self._client_socket.recv(msg_length - len(raw_msg))
            if chunk == b'':
                raise RuntimeError("Socket connection broken")
            raw_msg += chunk

        return Message.deserialize(raw_msg)
    
    
    def send_message(self, msg: bytes):
        """
        Sends a message to the client socket.
        """
        total_sent = 0
        msg_to_send = len(msg).to_bytes(MESSAGE_SIZE_BYTES, byteorder='big') + msg
        msg_length = len(msg_to_send)

        while total_sent < msg_length:
            sent = self._client_socket.send(msg_to_send[total_sent:])
            if sent == 0:
                raise RuntimeError("Socket connection broken")
            total_sent += sent
            
    def close(self):
        """
        Closes the agency socket
        """
        self._client_socket.close()

            