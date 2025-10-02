from pkg.message.constants import QUERY_4, MAX_PROFIT, MAX_QUANTITY
from pkg.message.utils import  parse_int, get_items_from_bytes, parse_float


class Q4IntermediateResult: 
    
    def __init__(self, year_month_created_at, item_id, quantity, subtotal):
        self.year_month_created_at = year_month_created_at
        self.item_id = item_id
        self.quantity = quantity
        self.subtotal = subtotal
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        year_month_created_at = parts[0]
        item_id = parse_int(parts[1])
        quantity = parse_int(parts[2])
        subtotal = parse_float(parts[3])
        return Q4IntermediateResult(year_month_created_at, item_id, quantity, subtotal)
    
    def get_q4_result_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """ 
        return get_items_from_bytes(data, Q4IntermediateResult)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.year_month_created_at};{self.item_id};{self.quantity};{self.subtotal}\n"
    
        
    
class Q4Result: 
    
    def __init__(self, year_month_created_at, item_data, value, result_type):
        self.year_month_created_at = year_month_created_at
        self.item_data = item_data
        self.value = value
        self.result_type = result_type
        self.query = QUERY_4
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        year_month_created_at = parts[0]
        item_data = parts[1]
        value = parts[2]
        result_type = parts[3]
        return Q4Result(year_month_created_at, item_data, value, result_type)
    
    def get_q4_result_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """ 
        return get_items_from_bytes(data, Q4Result)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.year_month_created_at};{self.item_data};{self.value};{self.result_type}\n"
    
    def join_item_name(self, item_name):
        self.item_data = item_name
        
    def pre_process(self):
        return {"qId": self.query, "year_month_created_at": self.year_month_created_at, "item_name": self.item_data, self.result_type: self.value }
