
def get_items_from_bytes(data: bytes, item_class):
    """
    Crea una lista de objetos a partir de bytes utilizando la clase proporcionada.
    :param data: Datos en bytes.
    :param item_class: Clase del objeto a crear (debe tener un método estático deserialize).
    :return: Lista de objetos.
    """
    items = []
    lines = data.decode('utf-8').split('\n')
    for line in lines:
        if line.strip():
            items.append(item_class.deserialize(line.encode('utf-8')))
    return items