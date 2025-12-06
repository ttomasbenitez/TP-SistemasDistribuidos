from worker.base import Worker 
from pkg.storage.state_storage.max_quantiry_profit import QuantityAndProfitStateStorage
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_2_RESULT, MESSAGE_TYPE_EOF, MAX_PROFIT, MAX_QUANTITY
from utils.custom_logging import initialize_log
import os
from pkg.message.q2_result import Q2Result
from Middleware.connection import PikaConnection
from utils.heartbeat import start_heartbeat_sender
from pkg.dedup.dedup_by_sender_strategy import DedupBySenderStrategy
from pkg.storage.state_storage.eof_storage import EofStorage

class QuantityAndProfit(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str, 
                 storage_dir: str,
                 host: str,
                 expected_acks: int = 2,
                 container_name: str = 'unknown-quantity-profit-node'):
        
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.connection = PikaConnection(host)
        self.state_storage = QuantityAndProfitStateStorage(storage_dir)
        self.dedup_strategy = DedupBySenderStrategy(storage_dir)
        self.eof_storage = EofStorage(storage_dir)
        self.expected_acks = expected_acks
        self.node_id = container_name
        
    def start(self):
        logging.info(f"action: startup | loading persisted state for recovery")
        
        self.state_storage.load_state_all()
        self.dedup_strategy.load_dedup_state()
        self.eof_storage.load_state_all()
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        
        def __on_message__(msg):
            try:
                message = Message.deserialize(msg)
                logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")
                            
                if message.type == MESSAGE_TYPE_EOF:
                    if self.on_eof_message(message, self.dedup_strategy, self.eof_storage, self.expected_acks):
                        logging.info(f"action: all_eofs_received | request_id: {message.request_id} | sending results")
                        self._send_results_by_date(message.request_id, data_output_queue)
                        self.eof_storage.delete_state(message.request_id)
                
                if self.dedup_strategy.is_duplicate(message):
                    logging.info(f"action: duplicated_message | request_id: {message.request_id}")
                    return
                
                items = message.process_message()
                if items:
                    self._accumulate_items(items, message.request_id)
            
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)
 
    def _accumulate_items(self, items, request_id):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        state = self.state_storage.get_state(request_id)
        items_by_ym = state.setdefault("items_by_ym", {})
        
        for it in items:
            ym = str(it.year_month_created_at)
            item_id = it.item_id
            
            if ym not in items_by_ym:
                items_by_ym[ym] = {}
            
            existing_item = items_by_ym[ym].get(item_id)
            if existing_item:
                old_qty = existing_item.quantity
                old_sub = existing_item.subtotal
                existing_item.quantity += it.quantity
                existing_item.subtotal += it.subtotal
                logging.debug(f"action: item_updated | ym: {ym} | item_id: {item_id} | qty: {old_qty}→{existing_item.quantity} | sub: {old_sub}→{existing_item.subtotal}")
            else:
                items_by_ym[ym][item_id] = it
                logging.debug(f"action: item_added | ym: {ym} | item_id: {item_id} | qty: {it.quantity} | sub: {it.subtotal}")
     
        self.state_storage.save_state(request_id)
            
        logging.info(f"action: state_persisted | request_id: {request_id}")
    
    def _send_results_by_date(self, request_id_of_eof, data_output_queue):
        chunk = ''
        
        if request_id_of_eof not in self.state_storage.data_by_request:
            logging.warning(f"action: send_results_no_data | request_id: {request_id_of_eof}")
            return

        data_for_request = self.state_storage.get_state(request_id_of_eof)
        items_by_ym = data_for_request.get("items_by_ym", {})
        
        logging.info(f"action: send_results_start | request_id: {request_id_of_eof} | yms_count: {len(items_by_ym)}")

        results_count = 0
        for ym, items_by_id in items_by_ym.items():
            if not items_by_id:
                continue

            max_item_quantity_id, max_item_quantity = max(
                items_by_id.items(), key=lambda kv: kv[1].quantity
            )

            max_item_profit_id, max_item_profit = max(
                items_by_id.items(), key=lambda kv: kv[1].subtotal
            )
            
            logging.debug(f"action: max_found | ym: {ym} | max_qty_item: {max_item_quantity_id} ({max_item_quantity.quantity}) | max_profit_item: {max_item_profit_id} ({max_item_profit.subtotal})")
            chunk += Q2Result(ym, max_item_quantity_id, max_item_quantity.quantity, MAX_QUANTITY).serialize()
            chunk += Q2Result(ym, max_item_profit_id, max_item_profit.subtotal, MAX_PROFIT).serialize()
            results_count += 2

        if chunk:
            logging.info(f"action: send_results_done | request_id: {request_id_of_eof} | results_count: {results_count} | CHUNK: {chunk}")
            message = Message(request_id_of_eof, MESSAGE_TYPE_QUERY_2_RESULT, 0, chunk, self.node_id)
            serialized_message = message.serialize()
            data_output_queue.send(serialized_message)
            logging.info(f"action: results_sent | msg_num: {0} | node_id: {self.node_id} | request_id: {request_id_of_eof}")
        else:
            logging.warning(f"action: send_results_empty | request_id: {request_id_of_eof} | no_results")
            
        self.state_storage.delete_state(request_id_of_eof)
        self.dedup_strategy.clean_dedup_state(request_id_of_eof)

    def close(self):
        try:
            self.heartbeat_sender.stop()
            self.connection.close()
            logging.info(f"action=close_connections | status=success")
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")

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
    config_params["expected_eofs"] = int(os.getenv('EXPECTED_EOFS', '2'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')
    config_params["container_name"] = os.getenv('CONTAINER_NAME', 'aggregator-quantity-profit')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    aggregator = QuantityAndProfit(
        config_params["input_queue"], 
        config_params["output_queue"], 
        config_params["storage_dir"], 
        config_params["rabbitmq_host"], 
        config_params["expected_eofs"],
        config_params["container_name"]
    )
    aggregator.start()

if __name__ == "__main__":
    main()