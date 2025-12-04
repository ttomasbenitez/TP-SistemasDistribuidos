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
import threading
from pkg.storage.state_storage.semester_agg import SemesterAggregatorStateStorage

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
        self.host = host
        self.state_storage = SemesterAggregatorStateStorage(storage_dir, {'agg_by_period': {}, 'last_by_sender': {}, 'eofs_by_request': 0})
        self.expected_eofs = expected_eofs
        
        # Message numbering: start at 0, incremental
        # Extract node_id from container name (e.g., "semester-aggregator-1" -> 1)
        try:
            self.node_id = int(container_name.split('-')[-1]) if container_name else 1
        except (ValueError, AttributeError, IndexError):
            self.node_id = 1
            logging.error(f"Could not parse node_id from {container_name}, defaulting to 1")
        
        self.msg_num_counter = 0
        
        # Dedup / sequencing state per sender
        self._sender_lock = threading.Lock()

    def start(self):
        # Load persisted state once on startup and hydrate last-msg map
        logging.info(f"action: startup | loading persisted state for recovery")
        self.state_storage.load_specific_state_all('last_by_sender')
        
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
                    return self._process_on_eof_message__(message, data_output_queue)
                
                # Dedup/ordering check
                if self.is_dupped(message, stream="data"):
                    return
                
                items = message.process_message()
                if items:
                    self._accumulate_items(items, message.request_id)
                    
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)

    def _process_on_eof_message__(self, message, data_output_queue):
        """Handle EOF message: track, send results when all EOFs received, cleanup."""
        state = self.state_storage.get_data_from_request(message.request_id)
        state["eofs_by_request"] = state.get("eofs_by_request", 0) + 1
        logging.info(f"EOF received | request_id: {message.request_id} | count: {state["eofs_by_request"]}/{self.expected_eofs}")
        
        if state["eofs_by_request"] < self.expected_eofs:
            return  # Wait for more EOFs
        
        self.state_storage.load_state(message.request_id)
        
        # All EOFs received, send results and forward EOF
        logging.info(f"All EOFs received, sending results | request_id: {message.request_id}")
        self._send_all_results(message.request_id, data_output_queue)
        data_output_queue.send(message.serialize())
        logging.info(f"EOF forwarded downstream | request_id: {message.request_id}")
        
        # Clean up
        del state["eofs_by_request"]
        self.state_storage.delete_state(message.request_id)

    def _accumulate_items(self, items, request_id):
        """
        Acumula cantidad por periodo y store, persistiendo estado incremental
        Similar a max_quantity_profit, mantenemos estado en memoria y persistimos en disco.
        """
        state = self.state_storage.get_data_from_request(request_id)
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
                
            logging.debug(f"action: agg_updated | period: {period} | store_id: {store_id} | amount: {amount} | old: {old_val} | new: {bucket[store_id]}")
            
        # Save to disk but keep data in memory (reset_state=False)
        self.state_storage.save_state(request_id)
        logging.info(f"action: state_persisted | request_id: {request_id}")

    def _send_all_results(self, request_id, data_output_queue):
        """Send all aggregated Q3 intermediate results."""
        try:
            if request_id not in self.state_storage.data_by_request:
                logging.warning(f"action: send_results_no_data | request_id: {request_id}")
                return

            data_for_request = self.state_storage.data_by_request[request_id]
            agg_by_period = data_for_request.get("agg_by_period", {})
            
            logging.info(f"action: send_results_start | request_id: {request_id} | periods_count: {len(agg_by_period)}")

            results_count = 0
            for period, store_map in agg_by_period.items():
                for store_id, total in store_map.items():
                    res = Q3IntermediateResult(period, store_id, total)
                    self._send_grouped_item(request_id, res, data_output_queue)
                    results_count += 1
            
            logging.info(f"action: send_results_done | request_id: {request_id} | results_count: {results_count}")
        except Exception as e:
            logging.error(f"action: send_results_error | request_id: {request_id} | error: {type(e).__name__}: {e}")

    def _send_grouped_item(self, request_id, item, data_output_queue):
        new_chunk = item.serialize()
        new_msg_num = self.msg_num_counter
        self.msg_num_counter += 1
        new_message = Message(request_id, MESSAGE_TYPE_QUERY_3_INTERMEDIATE_RESULT, new_msg_num, new_chunk)
        # Add node_id to message for dedup tracking per source
        new_message.add_node_id(self.node_id)
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
