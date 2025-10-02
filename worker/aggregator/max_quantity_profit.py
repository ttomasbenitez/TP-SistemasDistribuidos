from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import  MESSAGE_TYPE_QUERY_4_RESULT, MESSAGE_TYPE_EOF
from utils.custom_logging import initialize_log
import os
class QuantityAndProfit(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
        # Diccionario de tres niveles: year -> month -> item_data -> {"quantity", "subtotal"}
        self.data = dict()
        
    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                self._send_results_by_date()
                self.__received_EOF__(message)
                return
            items = message.process_message()
            if items:
                self._accumulate_items(items)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")
            
    def _accumulate_items(self, items):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        for it in items:
            ym = str(it.year_month_created_at)   # clave de mes
            item_data = it.item_data                 # usá un id estable, no un dict

            month_bucket = self.data.setdefault(ym, {})
            agg_item = month_bucket.get(item_data)

            if agg_item is None:
            # Tomamos el primer item como base (si querés, podés hacer una copia)
                month_bucket[item_data] = it
            else:
                agg_item.quantity += it.quantity
                agg_item.subtotal += it.subtotal
    
    def _send_results_by_date(self):
        chunk = ''

        # Si querés loguear "el primero", hacé una vista a lista
     # logging.info("LA DATAA %r", next(iter(self.data.items()), None))

        for ym, items_by_id in self.data.items():
        # items_by_id es un dict { item_data: ItemAcumulado }
            if not items_by_id:
                continue

        # Elegimos máximos en cantidad y en subtotal
            values = list(items_by_id.values())
            max_item_quantity = max(values, key=lambda x: x.quantity)
            max_item_profit   = max(values, key=lambda x: x.subtotal)

            # Serializamos (evitando duplicar si son el mismo ítem)
            chunk += max_item_quantity.serialize()
            chunk += max_item_profit.serialize()
            logging.info(f"{max_item_profit.serialize()} SERIALIZADO PROFIT {max_item_quantity.serialize()} QUANTITYYY")

        serialized_message = Message(0, MESSAGE_TYPE_QUERY_4_RESULT, 0, chunk).serialize()
        self.out_queue.send(serialized_message)
        logging.info(f"Enviado Q4 IntermediateResult | bytes={len(serialized_message)}")


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