from pkg.message.constants import QUERY_4
from pkg.message.utils import  parse_int, get_items_from_bytes, parse_float

class Q4Result: 
    
    def __init__(self, year_month_created_at, item_data, quantity, subtotal):
        self.year_month_created_at = year_month_created_at
        self.item_data = item_data
        self.quantity = quantity
        self.subtotal = subtotal
        self.query = QUERY_4
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        year_month_created_at = parts[0]
        item_data = parse_int(parts[1])
        quantity = parse_int(parts[2])
        subtotal = parse_float(parts[3])
        return Q4Result(year_month_created_at, item_data, quantity, subtotal)
    
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
        return f"{self.year_month_created_at};{self.item_data};{self.quantity};{self.subtotal}\n"
    
    def pre_process(self):
        
        return {"qId": self.query, "item_name": self.item_data, "sellings_qty": self.quantity, "profit_sum": self.subtotal}