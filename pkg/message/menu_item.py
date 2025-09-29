from pkg.message.constants import MESSAGE_CSV_MENU_ITEMS_AMOUNT
from pkg.message.utils import get_items_from_bytes, parse_int, parse_float

class MenuItem:
    """ 
    Clase para manejar items del menú.
    """

    def __init__(self, item_id, item_name, price):
        self.id = item_id
        self.name = item_name
        self.price = price

    def deserialize(data: bytes):
        """
        Crea un objeto MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto MenuItem.
        """
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_MENU_ITEMS_AMOUNT:
            raise ValueError("Datos inválidos para MenuItem")
        item_id = parse_int(parts[0])
        item_name = parts[1]
        price = parse_float(parts[3])
        return MenuItem(item_id, item_name, price)
    
    def get_menu_items_from_bytes(data: bytes):
        """
        Crea una lista de objetos MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos MenuItem.
        """
        return get_items_from_bytes(data, MenuItem)
    
    def serialize(self):
        """
        Serializa el objeto MenuItem a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.name};{self.price}\n"