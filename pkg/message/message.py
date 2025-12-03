from pkg.message.constants import (
    MESSAGE_TYPE_MENU_ITEMS,
    MESSAGE_TYPE_STORES,
    MESSAGE_TYPE_TRANSACTION_ITEMS,
    MESSAGE_TYPE_TRANSACTIONS,
    MESSAGE_TYPE_USERS,
    MESSAGE_TYPE_QUERY_1_RESULT,
    MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_3_RESULT,
    MESSAGE_TYPE_QUERY_2_RESULT,
    MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT,
    MESSAGE_TYPE_QUERY_4_RESULT,
    MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
)         
from pkg.message.menu_item import MenuItem
from pkg.message.store import Store
from pkg.message.transaction_item import TransactionItem
from pkg.message.transaction import Transaction
from pkg.message.user import User
from pkg.message.q1_result import Q1Result
from pkg.message.q3_result import Q3Result
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.q2_result import Q2Result, Q2IntermediateResult
from pkg.message.q4_result import Q4Result, Q4IntermediateResult
from pkg.message.utils import parse_int


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
        self.node_id = None
        self.content = content
        
    def add_node_id(self, node_id):
        """
        Agrega el ID del nodo al mensaje.
        :param node_id: ID del nodo.
        """
        self.node_id = node_id

    def serialize(self):
        """
        Serializa el mensaje en bytes.
        :return: Mensaje serializado en bytes.
        """
        node = self.node_id if self.node_id is not None else ''
        msg = f"{self.type};{self.request_id};{self.msg_num};{node};{self.content}".encode('utf-8')
        return msg
    
    def deserialize(raw_msg):
        """
        Deserializa el mensaje desde bytes.
        :param raw_msg: Mensaje en bytes.
        """
        raw_msg = raw_msg.decode('utf-8')
        parts = raw_msg.split(';', 4)
        type = int(parts[0])
        request_id = int(parts[1])
        msg_num = int(parts[2])
        node_id = parse_int(parts[3])
        content = parts[4]
        
        message = Message(request_id, type, msg_num, content)
        message.add_node_id(node_id)
        return message

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
        if self.type == MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT:
            return Q2IntermediateResult.get_q2_result_from_bytes(self.content)
        if self.type == MESSAGE_TYPE_QUERY_2_RESULT:
            return Q2Result.get_q2_result_from_bytes(self.content)
        if self.type == MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT:
            return Q4IntermediateResult.get_q4_result_from_bytes(self.content)
        if self.type == MESSAGE_TYPE_QUERY_4_RESULT:
            return Q4Result.get_q4_result_from_bytes(self.content)
        
    def update_content(self, new_content):
        """
        Actualiza el contenido del mensaje.
        :param new_content: Nuevo contenido del mensaje.
        """
        self.content = new_content

    def new_from_original(self, new_content, msg_num=None):
        """
        Crea un nuevo mensaje basado en este, pero con contenido actualizado.
        :param new_content: Contenido del nuevo mensaje.
        :param msg_num: Número de mensaje opcional. Si no se provee, se usa el del mensaje original.
        :return: Nuevo objeto Message independiente.
        """
        if msg_num is None:
            msg_num = self.msg_num
        return Message(self.request_id, self.type, msg_num, new_content)
    
    def get_node_id(self):
        """
        Obtiene el ID del nodo del mensaje.
        :return: ID del nodo.
        """
        return self.node_id