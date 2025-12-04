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
import threading

class QuantityAndProfit(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str, 
                 storage_dir: str,
                 host: str,
                 expected_eofs: int = 2,
                 container_name: str = None):
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.connection = PikaConnection(host)
        self.state_storage = QuantityAndProfitStateStorage(storage_dir)
        self.expected_eofs = expected_eofs
        self.eofs_by_request = {}
        
        # Message numbering: start at 0, incremental
        # Extract node_id from container name (e.g., "aggregator-quantity-profit-1" -> 1)
        try:
            self.node_id = int(container_name.split('-')[-1]) if container_name else 1
        except (ValueError, AttributeError, IndexError):
            self.node_id = 1
            logging.error(f"Could not parse node_id from {container_name}, defaulting to 1")
        
        self.msg_num_counter = 0
        
        # Dedup / sequencing state per sender
        self._sender_lock = threading.Lock()
        self._last_by_sender = {}
        
    def start(self):
        # Load persisted state once on startup and hydrate last-msg map
        logging.info(f"action: startup | loading persisted state for recovery")
        self.state_storage.load_state_all()
        for _rid, st in self.state_storage.data_by_request.items():
            for sid, num in st.get("last_by_sender", {}).items():
                self._last_by_sender[sid] = max(self._last_by_sender.get(sid, -1), num)
                logging.info(f"action: recovery_state_loaded | request_id: {_rid} | sender: {sid} | last_msg_num: {num}")
        
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
                
                #self._ensure_request(message.request_id)
                #self._inc_inflight(message.request_id)
                
                # Dedup/ordering check
                sender_id = message.get_node_id_and_request_id()
                with self._sender_lock:
                    last = self._last_by_sender.get(sender_id, -1)
                    logging.debug(f"action: dedup_check | sender:{sender_id} | msg_num:{message.msg_num} | last_seen:{last}")
                    if message.msg_num <= last:
                        if message.msg_num == last:
                            logging.warning(f"action: DUPLICATE_FILTERED | sender:{sender_id} | msg:{message.msg_num} | last:{last}")
                        else:
                            logging.warning(f"action: OUT_OF_ORDER_FILTERED | sender:{sender_id} | msg:{message.msg_num} < last:{last}")
                        return
                    self._last_by_sender[sender_id] = message.msg_num
                    logging.debug(f"action: msg_accepted | sender:{sender_id} | msg_num:{message.msg_num}")
                
                items = message.process_message()
                if items:
                    self._accumulate_items(items, message.request_id, sender_id, message.msg_num)
                
                #finally:
                #    self._dec_inflight(message.request_id)
                    
            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
        
        data_input_queue.start_consuming(__on_message__)
    
    def _process_on_eof_message__(self, message, data_output_queue):
        """Handle EOF message: track, send results when all EOFs received, cleanup."""
        self.eofs_by_request[message.request_id] = self.eofs_by_request.get(message.request_id, 0) + 1
        logging.info(f"EOF received | request_id: {message.request_id} | count: {self.eofs_by_request[message.request_id]}/{self.expected_eofs}")
        
        if self.eofs_by_request[message.request_id] < self.expected_eofs:
            return  # Wait for more EOFs
        
        # All EOFs received, send results and forward EOF
        logging.info(f"All EOFs received, sending results | request_id: {message.request_id}")
        self._send_results_by_date(message.request_id, data_output_queue)
        data_output_queue.send(message.serialize())
        logging.info(f"EOF forwarded downstream | request_id: {message.request_id}")
        
        # Clean up
        del self.eofs_by_request[message.request_id]
        self.state_storage.delete_state(message.request_id)
            
    def _accumulate_items(self, items, request_id, sender_id=None, msg_num=None):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        with self.state_storage._lock:
            state = self.state_storage.data_by_request.setdefault(request_id, {
                "items_by_ym": {},
                "last_by_sender": {},
            })
            
            items_by_ym = state["items_by_ym"]
            logging.info(f"action: accumulate_start | request_id: {request_id} | items_count: {len(items)} | sender: {sender_id} | msg_num: {msg_num}")
            
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
            
            # Persist last seen marker if provided
            if sender_id is not None and msg_num is not None:
                state["last_by_sender"][sender_id] = msg_num
            
            # Log final state
            total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
            total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
            logging.info(f"action: accumulate_done | request_id: {request_id} | total_qty: {total_qty} | total_sub: {total_sub} | yms: {len(items_by_ym)}")
        
        # Save to disk but keep data in memory (reset_state=False)
        self.state_storage.save_state(request_id, reset_state=False)
        logging.info(f"action: state_persisted | request_id: {request_id}")
    
    def _send_results_by_date(self, request_id_of_eof, data_output_queue):
        chunk = ''
        
        # Check if we have data for this request (already in memory from accumulation)
        if request_id_of_eof not in self.state_storage.data_by_request:
            logging.warning(f"action: send_results_no_data | request_id: {request_id_of_eof}")
            return

        data_for_request = self.state_storage.data_by_request[request_id_of_eof]
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
            logging.info(f"action: send_results_done | request_id: {request_id_of_eof} | results_count: {results_count}")
            new_msg_num = self.msg_num_counter
            self.msg_num_counter += 1
            message = Message(request_id_of_eof, MESSAGE_TYPE_QUERY_2_RESULT, new_msg_num, chunk)
            # Add node_id to message for dedup tracking per source
            message.add_node_id(self.node_id)
            serialized_message = message.serialize()
            data_output_queue.send(serialized_message)
            logging.info(f"action: results_sent | msg_num: {new_msg_num} | node_id: {self.node_id} | request_id: {request_id_of_eof}")
        else:
            logging.warning(f"action: send_results_empty | request_id: {request_id_of_eof} | no_results")

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