from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import  MESSAGE_TYPE_QUERY_2_RESULT, MESSAGE_TYPE_EOF, MAX_PROFIT, MAX_QUANTITY
from utils.custom_logging import initialize_log
import os
from pkg.message.q2_result import Q2Result
class QuantityAndProfit(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue):
        super().__init__(in_queue)
        self.out_queue = out_queue
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
            ym = str(it.year_month_created_at)
            item_id = it.item_id               

            month_bucket = self.data.setdefault(ym, {})
            agg_item = month_bucket.get(item_id)

            if agg_item is None:
                month_bucket[item_id] = it
            else:
                agg_item.quantity += it.quantity
                agg_item.subtotal += it.subtotal
    
    def _send_results_by_date(self):
        chunk = ''

        for ym, items_by_id in self.data.items():
            if not items_by_id:
                continue
            
            max_item_quantity_id, max_item_quantity = max(
                items_by_id.items(), key=lambda kv: kv[1].quantity
            )

            max_item_profit_id, max_item_profit = max(
                items_by_id.items(), key=lambda kv: kv[1].subtotal
            )
            
            chunk += Q2Result(ym, max_item_quantity_id, max_item_quantity.quantity, MAX_QUANTITY).serialize()
            chunk += Q2Result(ym, max_item_profit_id, max_item_profit.subtotal, MAX_PROFIT).serialize()
         
        serialized_message = Message(0, MESSAGE_TYPE_QUERY_2_RESULT, 0, chunk).serialize()
        self.out_queue.send(serialized_message)

    def close(self):
        try:
            self.in_middleware.close()
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