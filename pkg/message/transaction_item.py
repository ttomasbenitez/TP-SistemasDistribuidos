from pkg.message.constants import MESSAGE_CSV_TRANSACTION_ITEMS_AMOUNT
from pkg.message.utils import get_items_from_csv_bytes, get_items_from_bytes, parse_int, parse_date, parse_float

class TransactionItem:
    
    def __init__(self, item_id, quantity, subtotal, created_at):
        self.item_id = item_id
        self.quantity = quantity
        self.subtotal = subtotal
        self.created_at = created_at
        
    def deserialize_from_csv(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto TransactionItem.
        """
        
        parts = data.split(',')
        if len(parts) != MESSAGE_CSV_TRANSACTION_ITEMS_AMOUNT:
            raise ValueError("Datos inválidos para TransactionItem")
        item_id = parse_int(parts[1])
        quantity = parse_int(parts[2])
        subtotal = parse_float(parts[4])
        created_at = parse_date(parts[5])
        return TransactionItem(item_id, quantity, subtotal, created_at)
    
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto TransactionItem.
        """
        parts = data.split(';')
        item_id = parse_int(parts[0])
        quantity = parse_int(parts[1])
        subtotal = parse_float(parts[2])
        created_at = parse_date(parts[3])
        return TransactionItem(item_id, quantity, subtotal, created_at)
    
    def get_transaction_items_from_bytes(data: bytes, type):
        """
        Crea una lista de objetos TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos TransactionItem.
        """
        if type == 'csv': 
            return get_items_from_csv_bytes(data, TransactionItem)
        return get_items_from_bytes(data, TransactionItem)
    
    def serialize(self):
        """
        Serializa el objeto TransactionItem a bytes.
        :return: Datos en bytes.
        """
        return f"{self.item_id};{self.quantity};{self.subtotal};{self.created_at}\n"
    
    def get_year(self):
        """
        Obtiene el año de la transaccion
        :return: Año de registro o None si no está disponible.
        """
        if self.created_at:
            try:
                return self.created_at.year
            except AttributeError:
                return None
        return None
    
    def get_month(self):
        """
        Obtiene el mes de la transaccion
        :return: mes de registro o None si no está disponible.
        """
        if self.created_at:
            try:
                return self.created_at.month
            except AttributeError:
                return None
        return None
    
    def get_time(self):
        """
        Obtiene el horario de la transaccion
        :return: horario de la transaccion
        """
        return self.created_at.time()