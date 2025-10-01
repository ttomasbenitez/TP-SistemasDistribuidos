from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.transaction_item import TransactionItem
from pkg.message.constants import MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF
from utils.custom_logging import initialize_log
import os
from collections import defaultdict
from datetime import datetime

class QuantityAndProfit(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
        # Diccionario de tres niveles: year -> month -> item_id -> {"quantity", "subtotal"}
        self.data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"quantity": 0, "subtotal": 0.0})))
        
    def __on_message__(self, message):
        try:
            logging.info("Procesando mensaje")
            message = Message.read_from_bytes(message)
            if message.type == MESSAGE_TYPE_EOF:
                self._send_messages_per_item()
                self.__received_EOF__(message)
                return
            items = message.process_message()
            self._accumulate_items(items)
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize(), str(message.type))
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")
            
    def _accumulate_items(self, items):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        for item in items:
            year = item.created_at.year
            month = item.get_month()
            stats = self.data[item.item_id][year][month]
            stats["quantity"] += item.quantity
            stats["subtotal"] += item.subtotal
    
    def _send_messages_per_item(self):

        for item_id, year_dict in self.data.items():
            new_items = []
            for year, months in year_dict.items():
                for month, stats in months.items():
                    temp_item = TransactionItem(item_id, stats["quantity"], stats["subtotal"], datetime(year, month, 1))
                    new_items.append(temp_item)

            content = ''.join(item.serialize() for item in new_items)
            logging.info(f"Content: {content}")
            serialized_message = Message(0, MESSAGE_TYPE_TRANSACTION_ITEMS, 0, content).serialize()
            self.out_queue.send(serialized_message)
            logging.info(f"Enviado resumen item_id {item_id} con {len(new_items)} registros")

    def close(self):
        try:
            self.in_queue.close()
            self.out_queue.close()
        except Exception as e:
            print(f"Error al cerrar: {type(e).__name__}: {e}")

def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    
    aggregator = QuantityAndProfit(input_queue, output_queue)
    aggregator.start()

if __name__ == "__main__":
    main()