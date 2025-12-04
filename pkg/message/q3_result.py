from pkg.message.constants import QUERY_3
from pkg.message.utils import  parse_int, get_items_from_bytes, parse_float

class Q3IntermediateResult: 
    
    def __init__(self, year_half_created_at, store_id, tpv):
        self.year_half_created_at = year_half_created_at
        self.store_id = store_id
        self.intermediate_tpv = tpv
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        year_half_created_at = parts[0]
        store_id = parse_int(parts[1])
        intermediate_tpv = parse_float(parts[2])
        return Q3IntermediateResult(year_half_created_at, store_id, intermediate_tpv)
    
    def get_q3_result_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """ 
        return get_items_from_bytes(data, Q3IntermediateResult)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.year_half_created_at};{self.store_id};{self.intermediate_tpv}\n"
    
    def get_store(self):
        """
        Obtiene el id de la tienda
        :return: id de la tienda
        """
        return self.store_id
    
    def get_period(self):
        """
        Obtiene el id de la tienda
        :return: id de la tienda
        """
        return self.year_half_created_at
    
    def get_tpv(self):
        """
        Obtiene el total de productos vendidos
        :return: total de productos vendidos
        """
        return self.intermediate_tpv
    
class Q3Result:
    
    def __init__(self, year_half_created_at, store_name, tpv):
        self.year_half_created_at = year_half_created_at
        self.store_name = store_name
        self.tpv = tpv
        self.query = QUERY_3
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        year_half_created_at = parts[0]
        store_name = parts[1]
        tpv = parse_float(parts[2])
        return Q3Result(year_half_created_at, store_name, tpv)
    
    def get_q3_result_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """ 
        return get_items_from_bytes(data, Q3Result)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.year_half_created_at};{self.store_name};{self.tpv}\n"
    
    def pre_process(self):
        
        return {"qId": self.query, "period": self.year_half_created_at, "store_name": self.store_name, "tpv": self.tpv}