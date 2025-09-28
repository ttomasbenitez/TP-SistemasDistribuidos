from packages.messages.constants import MESSAGE_CSV_TRANSACTION_ITEMS_AMOUNT
from packages.messages.utils import get_items_from_bytes, parse_int

class TransactionItem:
    
    def __init__(self, id, item_id, created_at):
        self.transaction_id = id
        self.item_id = item_id
        self.created_at = created_at
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto TransactionItem.
        """
        
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_TRANSACTION_ITEMS_AMOUNT:
            raise ValueError("Datos inválidos para TransactionItem")
        transaction_id = parts[0]
        item_id = parse_int(parts[1])
        created_at = parts[5]
        return TransactionItem(transaction_id, item_id, created_at)
    
    def get_transaction_items_from_bytes(data: bytes):
        """
        Crea una lista de objetos TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos TransactionItem.
        """
        return get_items_from_bytes(data, TransactionItem)
    
    def serialize(self):
        """
        Serializa el objeto TransactionItem a bytes.
        :return: Datos en bytes.
        """
        return f"{self.transaction_id};{self.item_id};{self.created_at}\n"