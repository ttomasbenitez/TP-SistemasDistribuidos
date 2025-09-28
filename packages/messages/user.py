from packages.messages.constants import MESSAGE_CSV_USERS
from packages.messages.utils import get_items_from_bytes, parse_int

class User: 
    """
    Clase para manejar tiendas.
    """
    
    def __init__(self, user_id, birthdate):
        self.id = user_id
        self.birthdate = birthdate

    def deserialize(data: bytes):
        """
        Crea un objeto User a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto User.
        """
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_USERS:
            raise ValueError("Datos inválidos para User")
        user_id = parse_int(parts[0])
        birthdate = parts[2]
        return User(user_id, birthdate)
    
    def get_users_from_bytes(data: bytes):
        """
        Crea una lista de objetos User a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos User.
        """
        return get_items_from_bytes(data, User)
    
    def serialize(self):
        """
        Serializa el objeto User a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.birthdate}\n"