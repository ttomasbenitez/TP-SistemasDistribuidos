from pkg.message.constants import MESSAGE_CSV_TRANSACTIONS_AMOUNT
from pkg.message.utils import get_items_from_csv_bytes, parse_int, parse_float, parse_date, get_items_from_bytes
class Transaction:
    
    def __init__(self, transaction_id, store_id, user_id, final_amount, created_at):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.user_id = user_id
        self.final_amount = final_amount
        self.created_at = created_at
        
    def deserialize_from_csv(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        
        parts = data.split(',')
        if len(parts) != MESSAGE_CSV_TRANSACTIONS_AMOUNT:
            raise ValueError("Datos inválidos para Transaction")
        transaction_id = parts[0]
        store_id = parse_int(parts[1])
        user_id = parse_int(parse_float(parts[4]))
        final_amount = parse_float(parts[7])
        created_at = parse_date(parts[8])
        return Transaction(transaction_id, store_id, user_id, final_amount, created_at)
    
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        parts = data.split(';')
        transaction_id = parts[0]
        store_id = parse_int(parts[1])
        user_id = parse_int(parts[2])
        final_amount = parse_float(parts[3])
        created_at = parse_date(parts[4])
        return Transaction(transaction_id, store_id, user_id, final_amount, created_at)
    
    def get_transactions_from_bytes(data: bytes, type):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """
        if type == 'csv': 
            return get_items_from_csv_bytes(data, Transaction)
        return get_items_from_bytes(data, Transaction)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.transaction_id};{self.store_id};{self.user_id};{self.final_amount};{self.created_at}\n"
    
    def get_year(self):
        """
        Obtiene el año de la transaccion
        :return: Año de registro o None si no está disponible.
        """
        if self.created_at:
            try:
                return self.created_at.year
            except AttributeError:
                return None
        return None
    
    def get_semester(self):
        """
        Obtiene el semestre de la transaccion
        :return: Semestre de registro o None si no está disponible.
        """
        if self.created_at:
            try:
                month = self.created_at.month
                if month <= 6:
                    return 1
                else:
                    return 2
            except AttributeError:
                return None
        return None
    
    def get_store(self):
        """
        Obtiene el id de la tienda
        :return: id de la tienda
        """
        return self.store_id
    
    def get_final_amount(self):
        """
        Obtiene el monto final de la transaccion
        :return: monto final de la transaccion
        """
        return self.final_amount
    
    def get_time(self):
        """
        Obtiene el horario de la transaccion
        :return: horario de la transaccion
        """
        return self.created_at.time()
    
    def get_user(self):
        """
        Obtiene el id del usuario
        :return: id del usuario
        """
        return self.user_id
    