from pkg.message.constants import QUERY_1
from pkg.message.utils import get_items_from_csv_bytes, parse_int, parse_float, parse_date, get_items_from_bytes

class Q1Result:
    
    def __init__(self, transaction_id, final_amount):
        self.transaction_id = transaction_id
        self.final_amount = final_amount
        self.query = QUERY_1
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        transaction_id = parts[0]
        final_amount = parse_float(parts[1])
        return Q1Result(transaction_id, final_amount)
    
    def get_q1_result_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """ 
        return get_items_from_bytes(data, Q1Result)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.transaction_id};{self.final_amount}\n"
    
    def pre_process(self):
        
        return {"qId": self.query, "transaction_id": self.transaction_id, "final_amount": self.final_amount}
    
   