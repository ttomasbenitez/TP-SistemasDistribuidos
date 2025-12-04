from pkg.message.constants import QUERY_4
from pkg.message.utils import  parse_int, get_items_from_bytes, parse_float

class Q4IntermediateResult: 
    
    def __init__(self, store_id, birthdate, purchases_qty):
        self.store_id = store_id
        self.birthdate = birthdate
        self.purchases_qty = purchases_qty
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        store_id = parse_int(parts[0])
        birthdate = parts[1]
        purchases_qty = parse_int(parts[2])
        return Q4IntermediateResult(store_id, birthdate, purchases_qty)
    
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
        return f"{self.store_id};{self.birthdate};{self.purchases_qty}\n"
    
    def get_store(self):
        return self.store_id
    
    def get_birthdate(self):
        return self.birthdate
    
    def get_purchases_qty(self):
        return self.purchases_qty

    def get_query(self):
        return self.query

class Q4Result:
    
    def __init__(self, store_name, birthdate, purchases_qty):
        self.store_name = store_name
        self.birthdate = birthdate
        self.purchases_qty = purchases_qty
        self.query = QUERY_4
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        store_name = parts[0]
        birthdate = parts[1]
        purchases_qty = parse_int(parts[2])
        return Q4Result(store_name, birthdate, purchases_qty)
    
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
        return f"{self.store_name};{self.birthdate};{self.purchases_qty}\n"
    
    def pre_process(self):
        
        return {"qId": self.query, "store_name": self.store_name, "birthdate": self.birthdate, "purchases_qty": self.purchases_qty}