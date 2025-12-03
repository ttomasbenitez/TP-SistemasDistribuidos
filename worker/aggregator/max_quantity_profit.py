from worker.base import Worker 
from pkg.storage.state_storage.max_quantiry_profit import QuantityAndProfitStateStorage
from pkg.dedup.multi_source_dedup_strategy import MultiSourceDedupStrategy
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_2_RESULT, MESSAGE_TYPE_EOF, MAX_PROFIT, MAX_QUANTITY
from utils.custom_logging import initialize_log
import os
from pkg.message.q2_result import Q2Result
from Middleware.connection import PikaConnection
from utils.heartbeat import start_heartbeat_sender

class QuantityAndProfit(Worker):
    
    def __init__(self, 
                 data_input_queue: str, 
                 data_output_queue: str, 
                 storage_dir: str,
                 host: str,
                 expected_eofs: int = 2):
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.connection = PikaConnection(host)
        self.state_storage = QuantityAndProfitStateStorage(storage_dir)
        # Dedup storage in a subdirectory
        self.dedup = MultiSourceDedupStrategy(os.path.join(storage_dir, "dedup"))
        self.mensajes = 0
        self.mensajes_request = {}
        self.expected_eofs = expected_eofs
        self.eofs_by_request = {}
        
    def start(self):
        
        self.heartbeat_sender = start_heartbeat_sender()
        
        self.connection.start()
        self._consume_data_queue()
        self.connection.start_consuming()
        
    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        
        def __on_message__(message, ch, method):
            try:
                self.mensajes += 1
                message = Message.deserialize(message)
                self.mensajes_request[message.request_id] = self.mensajes_request.get(message.request_id, 0) + 1
                logging.info(f"CANTIDAD: {self.mensajes_request[message.request_id]} | request_id: {message.request_id}")
                if message.type == MESSAGE_TYPE_EOF:
                    logging.info(f"EOF recibido en data queue | request_id: {message.request_id}")
                    
                    # Track EOFs from multiple sources
                    self.eofs_by_request[message.request_id] = self.eofs_by_request.get(message.request_id, 0) + 1
                    logging.info(f"EOF count: {self.eofs_by_request[message.request_id]}/{self.expected_eofs} | request_id: {message.request_id}")
                    
                    if self.eofs_by_request[message.request_id] < self.expected_eofs:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return  # Wait for more EOFs
                    
                    # All EOFs received, send results and forward EOF
                    logging.info(f"All EOFs received, sending results | request_id: {message.request_id}")
                    self._send_results_by_date(message.request_id, data_output_queue)
                    data_output_queue.send(message.serialize())
                    logging.info(f"EOF enviado downstream | request_id: {message.request_id} | type: {message.type}")
                    
                    # Clean up EOF tracking and dedup state
                    del self.eofs_by_request[message.request_id]
                    self.dedup.update_state_on_eof(message)
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # Check dedup
                if self.dedup.check_state_before_processing(message) is False:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                logging.info(f"Mensaje recibido | request_id: {message.request_id} | type: {message.type}")
                items = message.process_message()
                if items:
                    self._accumulate_items(items, message.request_id)
                
                # Update dedup state
                self.dedup.update_contiguous_sequence(message)
                
                # ACK at the END after ALL state updates
                # This ensures atomicity: if we crash before ACK, the message will be
                # redelivered and fully reprocessed (accumulation is idempotent with +=)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag)
        
        data_input_queue.start_consuming(__on_message__, manual_ack=True)       
            
    def _accumulate_items(self, items, request_id):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        
        for it in items:
            ym = str(it.year_month_created_at)
            item_id = it.item_id               

            # Ensure structure exists
            if request_id not in self.state_storage.data_by_request:
                self.state_storage.data_by_request[request_id] = {}
            
            if ym not in self.state_storage.data_by_request[request_id]:
                 self.state_storage.data_by_request[request_id][ym] = {}

            month_bucket = self.state_storage.data_by_request[request_id][ym]
            agg_item = month_bucket.get(item_id)

            if agg_item is None:
                month_bucket[item_id] = it
            else:
                agg_item.quantity += it.quantity
                agg_item.subtotal += it.subtotal
            
        self.state_storage.save_state(request_id)
    
    def _send_results_by_date(self, request_id_of_eof, data_output_queue):
        chunk = ''
        
        self.state_storage.load_state(request_id_of_eof)
        # Check if we have data for this request
        if request_id_of_eof not in  self.state_storage.data_by_request:
            logging.warning(f"No hay datos para request_id: {request_id_of_eof}")
            return

        data_for_request = self.state_storage.data_by_request[request_id_of_eof]

        for ym, items_by_id in data_for_request.items():

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

        if chunk:
            logging.info(f"Enviando resultados acumulados | request_id: {request_id_of_eof}")
            serialized_message = Message(request_id_of_eof, MESSAGE_TYPE_QUERY_2_RESULT, 0, chunk).serialize()
            data_output_queue.send(serialized_message)
        
        logging.info(f"DATA ENVIADA | cantidad de mensajes: {self.mensajes} | request_id: {request_id_of_eof}")
        # Clean up state
        self.state_storage.delete_state(request_id_of_eof)

    #TODO
    def close(self):
        try:
            self.in_middleware.close()
            self.out_queue.close()
            self.eof_service_queue_middleware.close()
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
    config_params["expected_eofs"] = int(os.getenv('EXPECTED_EOFS', '2'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    aggregator = QuantityAndProfit(config_params["input_queue"], config_params["output_queue"], config_params["storage_dir"], config_params["rabbitmq_host"], config_params["expected_eofs"])
    aggregator.start()

if __name__ == "__main__":
    main()