from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from utils.custom_logging import initialize_log
from Middleware.connection import PikaConnection
import os
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT
from utils.heartbeat import start_heartbeat_sender
from pkg.storage.state_storage.semester_agg import SemesterAggregatorStateStorage
from pkg.storage.state_storage.eof_storage import EofStorage
from pkg.dedup.dedup_by_sender_strategy import DedupBySenderStrategy

class SemesterAggregator(Worker):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 storage_dir: str,
                 host: str,
                 expected_eofs: int = 2,
                 container_name: str = None):
        
        self.connection = PikaConnection(host)
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.node_id = container_name
        self.state_storage = SemesterAggregatorStateStorage(storage_dir)
        self.dedup_strategy = DedupBySenderStrategy(self.state_storage)
        self.eof_storage = EofStorage(storage_dir)
        self.expected_eofs = expected_eofs
        
    def start(self):
        # Load persisted state once on startup and hydrate last-msg map
        logging.info(f"action: startup | loading persisted state for recovery")
        self.state_storage.load_state_all()
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
                    if self.on_eof_message(message, self.dedup_strategy, self.eof_storage, self.expected_eofs):
                        logging.info(f"action: all_eofs_received | request_id: {message.request_id} | sending results")
                        self._send_all_results(message.request_id, data_output_queue)
                        self.dedup_strategy.clean_dedup_state(message.request_id)
                        self.eof_storage.delete_state(message.request_id)
                    return
                
                if self.dedup_strategy.is_duplicate(message):
                    logging.info(f"action: duplicated_message | request_id: {message.request_id}")
                    return
                
                items = message.process_message()
                if items:
                    self._accumulate_items(items, message.request_id)  
                    
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)

    def _accumulate_items(self, items, request_id, sender_id=None, msg_num=None):
        """
        Acumula cantidad por periodo y store, persistiendo estado incremental
        Similar a max_quantity_profit, mantenemos estado en memoria y persistimos en disco.
        """
        
        state = self.state_storage.get_state(request_id)
        agg_by_period = state["agg_by_period"]
        
        for it in items:
            year = it.get_year()
            sem  = it.get_semester()
            period = f"{year}-H{sem}"
            store_id = it.store_id
            amount = it.get_final_amount()
            
            bucket = agg_by_period.setdefault(period, {})
            old_val = bucket.get(store_id, 0.0)
            bucket[store_id] = old_val + amount
            
        self.state_storage.save_state(request_id)
        
        logging.info(f"action: state_persisted | request_id: {request_id}")

    def _send_all_results(self, request_id, data_output_queue):
        """Send all aggregated Q3 intermediate results."""
        try:
           
            if request_id not in self.state_storage.data_by_request:
                logging.warning(f"action: send_results_no_data | request_id: {request_id}")
                return
            
            state = self.state_storage.get_state(request_id)
            agg_by_period = state["agg_by_period"]
            
            logging.info(f"action: send_results_start | request_id: {request_id} | periods_count: {len(agg_by_period)}")

            msg_num = 0
            for period, store_map in agg_by_period.items():
                for store_id, total in store_map.items():
                    res = Q3IntermediateResult(period, store_id, total)
                    self._send_grouped_item(request_id, res, data_output_queue, msg_num)
                    msg_num += 1
            
            logging.info(f"action: send_results_done | request_id: {request_id}")
        except Exception as e:
            logging.error(f"action: send_results_error | request_id: {request_id} | error: {type(e).__name__}: {e}")

    def _send_grouped_item(self, request_id, item, data_output_queue, msg_num):
        new_chunk = item.serialize()
        new_message = Message(request_id, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, msg_num, new_chunk, self.node_id)
        data_output_queue.send(new_message.serialize())

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
    config_params = {
        "rabbitmq_host": os.getenv('RABBITMQ_HOST'),
        "input_queue": os.getenv('INPUT_QUEUE_1'),
        "output_queue": os.getenv('OUTPUT_QUEUE_1'),
        "logging_level": os.getenv('LOG_LEVEL', 'INFO'),
        "expected_eofs": int(os.getenv('EXPECTED_EOFS', '2')),
        "storage_dir": os.getenv('STORAGE_DIR', './data'),
        "container_name": os.getenv('CONTAINER_NAME', 'semester-aggregator'),
    }

    required_keys = [
        "rabbitmq_host",
        "input_queue",
        "output_queue",
    ]

    missing_keys = [key for key in required_keys if config_params[key] is None]
    if missing_keys:
        raise ValueError(f"Expected value(s) not found for: {', '.join(missing_keys)}. Aborting filter.")
    
    return config_params


def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    aggregator = SemesterAggregator(
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
