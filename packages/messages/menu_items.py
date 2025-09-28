from packages.messages.constants import MESSAGE_CSV_MENU_ITEMS_AMOUNT

class MenuItem:

    def __init__(self, item_id, item_name, price):
        self.item_id = item_id
        self.item_name = item_name
        self.price = price

    def __deserialize__(data: bytes):
        """
        Crea un objeto MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto MenuItem.
        """
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_MENU_ITEMS_AMOUNT:
            raise ValueError("Datos inválidos para MenuItem")
        item_id = int(parts[0])
        item_name = parts[1]
        price = float(parts[3])
        return MenuItem(item_id, item_name, price)
    
    def get_menu_items_from_bytes(data: bytes):
        """
        Crea una lista de objetos MenuItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos MenuItem.
        """
        items = []
        lines = data.decode('utf-8').split('\n')
        for line in lines:
            if line.strip():
                items.append(MenuItem.__deserialize__(line.encode('utf-8')))
        return items
    
    def serialize(self):
        """
        Serializa el objeto MenuItem a bytes.
        :return: Datos en bytes.
        """
        return f"{self.item_id};{self.item_name};{self.price}\n"