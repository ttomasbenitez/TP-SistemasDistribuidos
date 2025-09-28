from packages.messages.constants import MESSAGE_SIZE_BYTES, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_TRANSACTION_ITEMS
from packages.messages.menu_item import MenuItem
from packages.messages.store import Store
from packages.messages.transaction_item import TransactionItem

class Message:
    """
    Clase para manejar mensajes entre el cliente y el gateway.
    """

    def __init__(self, request_id, type, msg_num, content):
        """
        Inicializa el mensaje.
        :param request_id: ID de la solicitud.
        :param type: Tipo de mensaje.
        :param msg_num: Número de mensaje en la secuencia.
        :param content: Contenido del mensaje.
        """
        self.request_id = request_id
        self.type = type
        self.msg_num = msg_num
        self.content = content

    def send_message(self, socket):
        """
        Envía el mensaje a través del socket.
        :param socket: Socket a través del cual se envía el mensaje.
        """
        msg = self.__serialize__()
        total_sent = 0
        msg_length = len(msg)

        while total_sent < msg_length:
            sent = socket.send(msg[total_sent:])
            if sent == 0:
                raise RuntimeError("Socket connection broken")
            total_sent += sent

    def __serialize__(self):
        """
        Serializa el mensaje en bytes.
        :return: Mensaje serializado en bytes.
        """
        msg = f"{self.type};{self.request_id};{self.msg_num};{self.content}\n"
        msg = msg.encode('utf-8')
        msg = len(msg).to_bytes(MESSAGE_SIZE_BYTES, byteorder='big') + msg
        return msg
    
    def read_message(socket):
        """
        Lee un mensaje desde el socket y lo deserializa. Evita short reads.
        :param socket: Socket desde el cual se lee el mensaje.
        """
        raw_msg_length = b''
        while len(raw_msg_length) < MESSAGE_SIZE_BYTES:
            chunk = socket.recv(MESSAGE_SIZE_BYTES - len(raw_msg_length))
            if chunk == b'':
                raise RuntimeError("Socket connection broken")
            raw_msg_length += chunk

        msg_length = int.from_bytes(raw_msg_length, byteorder='big')
        raw_msg = b''
        while len(raw_msg) < msg_length:
            chunk = socket.recv(msg_length - len(raw_msg))
            if chunk == b'':
                raise RuntimeError("Socket connection broken")
            raw_msg += chunk

        return Message.__deserialize__(raw_msg)
    
    def __deserialize__(raw_msg):
        """
        Deserializa el mensaje desde bytes.
        :param raw_msg: Mensaje en bytes.
        """
        raw_msg = raw_msg.decode('utf-8')
        parts = raw_msg.split(';', 3)
        type = int(parts[0])
        request_id = int(parts[1])
        msg_num = int(parts[2])
        content = parts[3]
        return Message(request_id, type, msg_num, content)

    def proccess_message(self):
        """ 
        Procesea el mensaje basado en su tipo.
        :return: 
            - si el tipo es MESSAGE_TYPE_MENU_ITEMS, retorna una lista de MenuItem.
            - si el tipo es MESSAGE_TYPE_STORES, retorna una lista de Store.
        
        """
        if self.type == MESSAGE_TYPE_MENU_ITEMS:
            return MenuItem.get_menu_items_from_bytes(self.content.encode('utf-8'))
        if self.type == MESSAGE_TYPE_STORES:
            return Store.get_stores_from_bytes(self.content.encode('utf-8'))
        if self.type == MESSAGE_TYPE_TRANSACTION_ITEMS:
            return TransactionItem.get_transaction_items_from_bytes(self.content.encode('utf-8'))