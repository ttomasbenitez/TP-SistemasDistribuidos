from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_QUERY_2_RESULT, MESSAGE_TYPE_EOF, MAX_PROFIT, MAX_QUANTITY
from utils.custom_logging import initialize_log
import os
from pkg.message.q2_result import Q2Result, Q2IntermediateResult

class QuantityAndProfit(Worker):
    
    def __init__(self, in_queue: MessageMiddlewareQueue, out_queue: MessageMiddlewareQueue, storage_dir: str):
        super().__init__(in_queue)
        self.out_queue = out_queue
        self.storage_dir = storage_dir
        # Structure: {request_id: {ym: {item_id: Q2IntermediateResult}}}
        self.data_by_request = dict()
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            
        self._load_state()
        
    def _load_state(self):
        """Carga el estado desde los archivos en el directorio de almacenamiento."""
        logging.info(f"Cargando estado desde {self.storage_dir}")
        for filename in os.listdir(self.storage_dir):
            if not filename.endswith(".txt"):
                continue
                
            try:
                request_id = int(filename.split(".")[0])
                filepath = os.path.join(self.storage_dir, filename)
                
                with open(filepath, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Format: ym;item_id;quantity;subtotal                        
                        parts = line.split(";", 1)
                        ym = parts[0]
                        item_data = parts[1]
                        
                        item = Q2IntermediateResult.deserialize(item_data)
                        
                        if request_id not in self.data_by_request:
                            self.data_by_request[request_id] = {}
                        
                        if ym not in self.data_by_request[request_id]:
                            self.data_by_request[request_id][ym] = {}
                            
                        self.data_by_request[request_id][ym][item.item_id] = item
                        
                logging.info(f"Estado cargado para request_id: {request_id}")
            except Exception as e:
                logging.error(f"Error al cargar estado de {filename}: {e}")

    def _save_state(self, request_id):
        """Guarda el estado de un request_id atomicamente."""
        if request_id not in self.data_by_request:
            return

        temp_filepath = os.path.join(self.storage_dir, f"{request_id}.tmp")
        final_filepath = os.path.join(self.storage_dir, f"{request_id}.txt")
        
        try:
            with open(temp_filepath, "w") as f:
                for ym, items_by_id in self.data_by_request[request_id].items():
                    for item in items_by_id.values():
                        # Format: ym;item.serialize()
                        f.write(f"{ym};{item.serialize()}")
                f.flush()
                os.fsync(f.fileno())
            
            os.rename(temp_filepath, final_filepath)
            # logging.debug(f"Estado guardado para request_id: {request_id}")
        except Exception as e:
            logging.error(f"Error al guardar estado para request_id {request_id}: {e}")
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)

    def _delete_state(self, request_id):
        """Borra el estado de un request_id de memoria y disco."""
        if request_id in self.data_by_request:
            del self.data_by_request[request_id]
            
        filepath = os.path.join(self.storage_dir, f"{request_id}.txt")
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                logging.info(f"Estado borrado para request_id: {request_id}")
            except Exception as e:
                logging.error(f"Error al borrar archivo de estado {filepath}: {e}")

    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)
            if message.type == MESSAGE_TYPE_EOF:
                logging.info(f"EOF recibido en data queue | request_id: {message.request_id}")
                self._send_results_by_date(message.request_id)
                self.__received_EOF__(message)
                return

            logging.info(f"Mensaje recibido | request_id: {message.request_id} | type: {message.type}")
            items = message.process_message()
            if items:
                self._accumulate_items(items, message.request_id)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

    def __received_EOF__(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")
            
    def _accumulate_items(self, items, request_id):
        """
        Acumula cantidad y subtotal por año, mes y producto
        """
        updated = False
        for it in items:
            ym = str(it.year_month_created_at)
            item_id = it.item_id               

            # Ensure structure exists
            if request_id not in self.data_by_request:
                self.data_by_request[request_id] = {}
            
            if ym not in self.data_by_request[request_id]:
                self.data_by_request[request_id][ym] = {}

            month_bucket = self.data_by_request[request_id][ym]
            agg_item = month_bucket.get(item_id)

            if agg_item is None:
                month_bucket[item_id] = it
            else:
                agg_item.quantity += it.quantity
                agg_item.subtotal += it.subtotal
            updated = True
            
        if updated:
            self._save_state(request_id)
    
    def _send_results_by_date(self, request_id_of_eof):
        chunk = ''

        # Check if we have data for this request
        if request_id_of_eof not in self.data_by_request:
            logging.warning(f"No hay datos para request_id: {request_id_of_eof}")
            return

        data_for_request = self.data_by_request[request_id_of_eof]

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
            self.out_queue.send(serialized_message)
        
        # Clean up state
        self._delete_state(request_id_of_eof)

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
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    
    aggregator = QuantityAndProfit(input_queue, output_queue, config_params["storage_dir"])
    aggregator.start()

if __name__ == "__main__":
    main()