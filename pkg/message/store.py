from pkg.message.constants import MESSAGE_CSV_STORES_AMOUNT
from pkg.message.utils import get_items_from_csv_bytes, get_items_from_bytes, parse_int

class Store: 
    """
    Clase para manejar tiendas.
    """
    
    def __init__(self, store_id, store_name):
        self.id = store_id
        self.name = store_name

    def deserialize_from_csv(data: bytes):
        """
        Crea un objeto Store a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Store.
        """
        parts = data.split(',')
        if len(parts) != MESSAGE_CSV_STORES_AMOUNT:
            raise ValueError("Datos inválidos para Store")
        store_id = parse_int(parts[0])
        store_name = parts[1]
        return Store(store_id, store_name)
    
    def deserialize(data: bytes):
        """
        Crea un objeto Store a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Store.
        """
        parts = data.split(';')
        store_id = parse_int(parts[0])
        store_name = parts[1]
        return Store(store_id, store_name)
    
    def get_stores_from_bytes(data: bytes, type):
        """
        Crea una lista de objetos Store a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Store.
        """
        if type == 'csv': 
            return get_items_from_csv_bytes(data, Store)
        return get_items_from_bytes(data, Store)
    
    def serialize(self):
        """
        Serializa el objeto Store a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.name}\n"