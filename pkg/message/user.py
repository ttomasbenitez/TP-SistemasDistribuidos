from pkg.message.constants import MESSAGE_CSV_USERS
from pkg.message.utils import get_items_from_csv_bytes, parse_int, parse_date, get_items_from_bytes

class User: 
    """
    Clase para manejar tiendas.
    """
    
    def __init__(self, user_id, birthdate):
        self.id = user_id
        self.birthdate = birthdate

    def deserialize_from_csv(data: bytes):
        """
        Crea un objeto User a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto User.
        """
        parts = data.split(',')
        if len(parts) != MESSAGE_CSV_USERS:
            raise ValueError("Datos inválidos para User")
        user_id = parse_int(parts[0])
        birthdate = parts[2]
        return User(user_id, birthdate)
    
    def deserialize(data: bytes):
        """
        Crea un objeto User a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto User.
        """
        parts = data.split(';')
        user_id = parse_int(parts[0])
        birthdate = parts[1]
        return User(user_id, birthdate)
    
    def get_users_from_bytes(data: bytes, type):
        """
        Crea una lista de objetos User a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos User.
        """
        if type == 'csv': 
            return get_items_from_csv_bytes(data, User)
        return get_items_from_bytes(data, User)
    
    def serialize(self):
        """
        Serializa el objeto User a bytes.
        :return: Datos en bytes.
        """
        return f"{self.id};{self.birthdate}\n"
    
    
    def get_user_id(self):
        """
        Obtiene el año de registro del usuario.
        :return: Año de registro o None si no está disponible.
        """
        return self.id
    
    def get_birthdate(self):
        """
        Obtiene la fecha de nacimiento del usuario.
        :return: Fecha de nacimiento o None si no está disponible.
        """
        return self.birthdate