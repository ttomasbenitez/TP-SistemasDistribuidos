from pkg.message.constants import MESSAGE_CSV_MENU_ITEMS_AMOUNT
from pkg.message.utils import get_items_from_csv_bytes, parse_int, parse_float, get_items_from_bytes

class MenuItem:
    """ 
    Clase para manejar items del menú.
    """

    def __init__(self, item_id, item_name):
        self.id = item_id
        self.name = item_name

    def deserialize_from_csv(data: bytes):
        """
        Crea un objeto MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto MenuItem.
        """
        parts = data.split(',')
        if len(parts) != MESSAGE_CSV_MENU_ITEMS_AMOUNT:
            raise ValueError("Datos inválidos para MenuItem")
        item_id = parse_int(parts[0])
        item_name = parts[1]
        return MenuItem(item_id, item_name)
    
    def deserialize(data: bytes):
        """
        Crea un objeto MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto MenuItem.
        """
        parts = data.split(';')
        item_id = parse_int(parts[0])
        item_name = parts[1]
        return MenuItem(item_id, item_name)
    
    def get_menu_items_from_bytes(data: bytes, type):
        """
        Crea una lista de objetos MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos MenuItem.
        """
        if type == 'csv':
            return get_items_from_csv_bytes(data, MenuItem)
        return get_items_from_bytes(data, MenuItem)
    
    def serialize(self):
        """
        Serializa el objeto MenuItem a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.name}\n"
    
    def get_id(self):
        return self.id
    
    def get_name(self):
        return self.name