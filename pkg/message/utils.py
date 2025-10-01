import datetime
import logging

def get_items_from_csv_bytes(data: bytes, item_class):
    """
    Crea una lista de objetos a partir de bytes utilizando la clase proporcionada.
    :param data: Datos en bytes.
    :param item_class: Clase del objeto a crear (debe tener un método estático deserialize).
    :return: Lista de objetos.
    """
    items = []
    lines = data.split('\n')
    for line in lines:
        if line.strip():
            items.append(item_class.deserialize_from_csv(line))
    return items

def get_items_from_bytes(data: bytes, item_class):
    """
    Crea una lista de objetos a partir de bytes utilizando la clase proporcionada.
    :param data: Datos en bytes.
    :param item_class: Clase del objeto a crear (debe tener un método estático deserialize).
    :return: Lista de objetos.
    """
    items = []
    lines = data.rstrip('\n').split('\n')
    
    for line in lines:
        if line.strip():
            items.append(item_class.deserialize(line))
    return items

def parse_int(part):
    """
    Parsea un entero de una cadena, devolviendo None si la cadena está vacía o no es válida.
    :param part: Cadena a parsear.
    :return: Entero o None.
    """
    try:
        return int(part)
    except (ValueError, TypeError):
        return None
    
def parse_float(part):
    """
    Parsea un float de una cadena, devolviendo None si la cadena está vacía o no es válida.
    :param part: Cadena a parsear.
    :return: Float o None.
    """
    try:
        return float(part)
    except (ValueError, TypeError):
        return None
    
def parse_date(part):
    """
    Parsea una fecha de una cadena en formato ISO, devolviendo None si la cadena está vacía o no es válida.
    :param part: Cadena a parsear.
    :return: datetime.date o None. 
    """
    try:
        return datetime.datetime.fromisoformat(part)
    except (ValueError, TypeError):
        return None