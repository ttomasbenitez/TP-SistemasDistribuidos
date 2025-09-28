from packages.messages.constants import MESSAGE_CSV_STORES_AMOUNT

class Store: 
    """
    Clase para manejar tiendas.
    """
    
    def __init__(self, store_id, store_name):
        self.id = store_id
        self.name = store_name

    def __deserialize__(data: bytes):
        """
        Crea un objeto Store a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Store.
        """
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_STORES_AMOUNT:
            raise ValueError("Datos inválidos para Store")
        store_id = int(parts[0])
        store_name = parts[1]
        return Store(store_id, store_name)
    
    def get_stores_from_bytes(data: bytes):
        """
        Crea una lista de objetos Store a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Store.
        """
        items = []
        lines = data.decode('utf-8').split('\n')
        for line in lines:
            if line.strip():
                items.append(Store.__deserialize__(line.encode('utf-8')))
        return items
    
    def serialize(self):
        """
        Serializa el objeto Store a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.name}\n"