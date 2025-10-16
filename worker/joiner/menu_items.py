import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_QUERY_2_RESULT
from utils.custom_logging import initialize_log
from multiprocessing import Process, Value, Manager
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
import os
from multiprocessing import Process, Manager, Value
from pkg.message.utils import parse_int

EXPECTED_EOFS = 2

class JoinerMenuItems:
    def __init__(self, data_input_queue: MessageMiddlewareQueue, 
                 menu_items_input_queue: MessageMiddlewareQueue, 
                output_exchange: MessageMiddlewareExchange,
                 out_queue_prefix: str):
        super().__init__()
        self.data_input_queue = data_input_queue
        self.menu_items_input_queue = menu_items_input_queue
        self.output_exchange = output_exchange
        self.out_queue_prefix = out_queue_prefix

        self.menu_items = {}
        self.clients = []
        self.pending_items = []
        self.eofs_by_client = {}


    def __on_message__(self, message):
        try:
            logging.info("Procesando mensaje")
            message = Message.deserialize(message)

            if message.request_id not in self.clients:
                out_queue_name = f"{self.out_queue_prefix}_{message.request_id}"
                self.output_exchange.add_queue_to_exchange(out_queue_name, str(message.request_id))
                self.clients.append(message.request_id)

            if message.type == MESSAGE_TYPE_EOF:
                self._process_pending(message.request_id)
                return

            items = message.process_message()

            if message.type == MESSAGE_TYPE_QUERY_2_RESULT:
                ready_to_send = ''
                for item in items:
                    name = self.menu_items.get(parse_int(item.item_data))
                    if name:
                        item.join_item_name(name)
                        ready_to_send += item.serialize()
                    else:
                        self.pending_items.append(item, message.request_id)

                if ready_to_send:
                    logging.info("READY TO SEND")
                    message.update_content(ready_to_send)
                    serialized = message.serialize()
                    self.output_exchange.send(serialized, str(message.request_id))
                    logging.info(f"Sending message: {serialized}")

        except Exception as e:
            logging.error(f"Error en JoinerMenuItems: {e}")

        finally:
            try:
                self.in_middleware.stop_consuming()
            except Exception as e:
                logging.info(f"stop_consuming ignorado: {type(e).__name__}: {e}")
            finally:
                try:
                    self.in_middleware.close()
                except Exception:
                    pass

    def _process_pending(self, request_id):
        ready_to_send = ''
        for (item, req_id) in list(self.pending_items):
            if req_id != request_id:
                continue
            name = self.menu_items.get(parse_int(item.item_data))
            if name:
                item.join_item_name(name)
                ready_to_send += item.serialize()
                self.pending_items.remove((item, req_id))

        if ready_to_send:
            serialized = Message(request_id, MESSAGE_TYPE_QUERY_2_RESULT, 0, ready_to_send).serialize()
            self.output_exchange.send(serialized, str(request_id))
            logging.info(f"Sending message: {serialized}")

    def close(self):
        try:
            self.output_exchange.close()
            self.data_input_queue.close()
            self.menu_items_input_queue.close()
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
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue_prefix"] = os.getenv('OUTPUT_QUEUE_PREFIX')
    config_params["output_exchange_q2"] = os.getenv('OUTPUT_EXCHANGE')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in (config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_exchange_q2"]):
        raise ValueError("Expected value not found. Aborting.")
    
    return config_params
def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    output_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["output_exchange_q2"], {})

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    menu_items_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_2"])

    aggregator = JoinerMenuItems(data_input_queue, menu_items_input_queue, output_exchange, config_params["output_queue_prefix"])
    aggregator.start()

if __name__ == "__main__":
    main()
