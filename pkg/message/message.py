from pkg.message.constants import (
    MESSAGE_TYPE_MENU_ITEMS,
    MESSAGE_TYPE_STORES,
    MESSAGE_TYPE_TRANSACTION_ITEMS,
    MESSAGE_TYPE_TRANSACTIONS,
    MESSAGE_TYPE_USERS,
    MESSAGE_TYPE_QUERY_1_RESULT,
    MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_3_RESULT
)         
from pkg.message.menu_item import MenuItem
from pkg.message.store import Store
from pkg.message.transaction_item import TransactionItem
from pkg.message.transaction import Transaction
from pkg.message.user import User
from pkg.message.q1_result import Q1Result
from pkg.message.q3_result import Q3Result
from pkg.message.q3_result import Q3IntermediateResult
import logging


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

    def serialize(self):
        """
        Serializa el mensaje en bytes.
        :return: Mensaje serializado en bytes.
        """
        msg = f"{self.type};{self.request_id};{self.msg_num};{self.content}".encode('utf-8')
        return msg
    
    def deserialize(raw_msg):
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

    def process_message_from_csv(self):
        """ 
        Procesea el mensaje basado en su tipo.
        :return: si el tipo es 
            - MESSAGE_TYPE_MENU_ITEMS, retorna una lista de MenuItem.
            - MESSAGE_TYPE_STORES, retorna una lista de Store.
            - MESSAGE_TYPE_TRANSACTION_ITEMS, retorna una lista de TransactionItem.
            - MESSAGE_TYPE_TRANSACTIONS, retorna una lista de Transaction.
        """
        return self.__process_message_by_type('csv')
        
    def process_message(self):
        """ 
        Procesea el mensaje basado en su tipo.
        :return: si el tipo es 
            - MESSAGE_TYPE_MENU_ITEMS, retorna una lista de MenuItem.
            - MESSAGE_TYPE_STORES, retorna una lista de Store.
            - MESSAGE_TYPE_TRANSACTION_ITEMS, retorna una lista de TransactionItem.
            - MESSAGE_TYPE_TRANSACTIONS, retorna una lista de Transaction.
        """
        return self.__process_message_by_type('')
        
    def __process_message_by_type(self, type):
        """ 
        Procesea el mensaje basado en su tipo.
        :return: si el tipo es 
            - MESSAGE_TYPE_MENU_ITEMS, retorna una lista de MenuItem.
            - MESSAGE_TYPE_STORES, retorna una lista de Store.
            - MESSAGE_TYPE_TRANSACTION_ITEMS, retorna una lista de TransactionItem.
            - MESSAGE_TYPE_TRANSACTIONS, retorna una lista de Transaction.
        """
        if self.type == MESSAGE_TYPE_MENU_ITEMS:
            return MenuItem.get_menu_items_from_bytes(self.content, type)
        if self.type == MESSAGE_TYPE_STORES:
            return Store.get_stores_from_bytes(self.content, type)
        if self.type == MESSAGE_TYPE_TRANSACTION_ITEMS:
            return TransactionItem.get_transaction_items_from_bytes(self.content, type)
        if self.type == MESSAGE_TYPE_TRANSACTIONS:
            return Transaction.get_transactions_from_bytes(self.content, type)
        if self.type == MESSAGE_TYPE_USERS:
            return User.get_users_from_bytes(self.content, type)
        if self.type == MESSAGE_TYPE_QUERY_1_RESULT:
            return Q1Result.get_q1_result_from_bytes(self.content)
        if self.type == MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT:
            return Q3IntermediateResult.get_q3_result_from_bytes(self.content)
        if self.type == MESSAGE_TYPE_QUERY_3_RESULT:
            return Q3Result.get_q3_result_from_bytes(self.content)
        
    def update_content(self, new_content):
        """
        Actualiza el contenido del mensaje.
        :param new_content: Nuevo contenido del mensaje.
        """
        self.content = new_content

    def new_from_original(self, new_content):
        """
        Crea un nuevo mensaje basado en este, pero con contenido actualizado.
        :param new_content: Contenido del nuevo mensaje.
        :return: Nuevo objeto Message independiente.
        """
        return Message(self.request_id, self.type, self.msg_num, new_content)