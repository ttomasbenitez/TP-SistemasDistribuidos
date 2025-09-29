from pkg.message.constants import MESSAGE_CSV_TRANSACTIONS_AMOUNT
from pkg.message.utils import get_items_from_bytes, parse_int, parse_float, parse_date
class Transaction:
    
    def __init__(self, transaction_id, store_id, user_id, final_amount, created_at):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.user_id = user_id
        self.final_amount = final_amount
        self.created_at = created_at
        
    def deserialize(data: bytes):
        """ 
        Crea un objeto TransactionItem a partir de bytes.
        :param data: Datos en bytes.
        :return: Objeto Transaction.
        """
        
        parts = data.decode('utf-8').split(',')
        if len(parts) != MESSAGE_CSV_TRANSACTIONS_AMOUNT:
            raise ValueError("Datos inválidos para Transaction")
        transaction_id = parts[0]
        store_id = parse_int(parts[1])
        user_id = parse_int(parse_float(parts[4]))
        final_amount = parse_float(parts[7])
        created_at = parse_date(parts[8])
        return Transaction(transaction_id, store_id, user_id, final_amount, created_at)
    
    def get_transactions_from_bytes(data: bytes):
        """
        Crea una lista de objetos Transaction a partir de bytes.
        :param data: Datos en bytes.
        :return: Lista de objetos Transaction.
        """
        return get_items_from_bytes(data, Transaction)
    
    def serialize(self):
        """
        Serializa el objeto Transaction a bytes.
        :return: Datos en bytes.
        """
        return f"{self.transaction_id};{self.store_id};${self.user_id};{self.final_amount};{self.created_at}\n"