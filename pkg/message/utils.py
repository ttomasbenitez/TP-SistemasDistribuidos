import datetime
from pkg.message.constants import SUB_MESSAGE_MULTIPLIER

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
        return ''
    
def parse_float(part):
    """
    Parsea un float de una cadena, devolviendo None si la cadena está vacía o no es válida.
    :param part: Cadena a parsear.
    :return: Float o None.
    """
    try:
        return float(part)
    except (ValueError, TypeError):
        return ''
    
def parse_date(part):
    """
    Parsea una fecha de una cadena en formato ISO, devolviendo None si la cadena está vacía o no es válida.
    :param part: Cadena a parsear.
    :return: datetime.date o None. 
    """
    try:
        return datetime.datetime.fromisoformat(part)
    except (ValueError, TypeError):
        return ''

def calculate_sub_message_id(original_id, sub_id):
    """
    Calcula el ID del sub-mensaje basado en el ID original y el sub-ID.
    :param original_id: ID del mensaje original.
    :param sub_id: ID del sub-mensaje.
    :return: ID del sub-mensaje calculado.
    """
    return original_id * SUB_MESSAGE_MULTIPLIER + sub_id