import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_QUERY_4_RESULT
from utils.custom_logging import initialize_log
from multiprocessing import Process, Value, Manager
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS
from Middleware.middleware import MessageMiddlewareQueue
import os
from multiprocessing import Process, Manager, Value
from pkg.message.utils import parse_int

EXPECTED_EOFS = 2

class JoinerMenuItems:
    def __init__(self, expected_acks: int, data_input_queue: MessageMiddlewareQueue, menu_items_input_queue: MessageMiddlewareQueue, data_out_queue: MessageMiddlewareQueue, eof_in_queues: list[MessageMiddlewareQueue], eof_out_queues: list[MessageMiddlewareQueue]):
        self.data_input_queue = data_input_queue
        self.menu_items_input_queue = menu_items_input_queue
        self.data_out_queue = data_out_queue
        self.eof_in_queues = eof_in_queues
        self.eof_out_queues = eof_out_queues

        self.manager = Manager()
        self.menu_items = self.manager.dict()
        self.pending_items = []

        self.eof_received = Value('b', False)
        self.leader = Value('b', expected_acks == 0)
        self.processing_data = Value('b', False)
        self.expected_acks = Value('i', expected_acks)
        self.received_menu_items_eof = Value('b', False)

    def start(self):
        procs = []

        procs.append(Process(target=self._consume_data_queue, args=(self.data_input_queue,)))
        procs.append(Process(target=self._consume_menu_items_queue, args=(self.menu_items_input_queue,)))
        for q in self.eof_in_queues:
            procs.append(Process(target=self._consume_eof_queue, args=(q,)))

        for p in procs:
            p.start()
        for p in procs:
            p.join()

    def _consume_menu_items_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self.__on_menu_item_message__)

    def _consume_data_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self.__on_message__)

    def _consume_eof_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self._on_eof_from_queue_message)

    def _on_eof_from_queue_message(self, message):
        try:
            logging.info("Procesando mensaje de EOF queue")
            message = Message.deserialize(message)
        
            with self.leader.get_lock():
                is_leader = self.leader.value

            if is_leader:
                with self.expected_acks.get_lock():
                    if self.expected_acks.value > 0:
                       self.expected_acks.value -= 1
                    if self.expected_acks.value <= 0:
                        self._send_final_eof(message)
            else:
                with self.processing_data.get_lock(), self.eof_received.get_lock():
                    if message.type == MESSAGE_TYPE_EOF and not self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        self.__received_EOF__(message)
                        logging.info("Procesamiento finalizado")
                        return
                    elif message.type == MESSAGE_TYPE_EOF and self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        return
                    
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

    def __on_menu_item_message__(self, message):
        logging.info("Procesando mensaje de MenuItems")
        message = Message.deserialize(message)

        if message.type == MESSAGE_TYPE_EOF:
            with self.received_menu_items_eof.get_lock():
                self.received_menu_items_eof.value = True
            return

        items = message.process_message()
        if message.type == MESSAGE_TYPE_MENU_ITEMS:
            for item in items:
                self.menu_items[item.get_id()] = item.get_name()

    def __on_message__(self, message):
        try:
            logging.info("Procesando mensaje")
            message = Message.deserialize(message)

            if message.type == MESSAGE_TYPE_EOF:
                with self.leader.get_lock(), self.received_menu_items_eof.get_lock():
                    if self.leader.value and self.received_menu_items_eof.value:
                        self._process_pending()
                        self._send_final_eof(message)
                    else: 
                        self.leader.value = True
                        self.__received_EOF__(message)
                return

            with self.processing_data.get_lock():
                self.processing_data.value = True
            items = message.process_message()

            if message.type == MESSAGE_TYPE_QUERY_4_RESULT:
                ready_to_send = ''
                for item in items:
                    name = self.menu_items.get(parse_int(item.item_data))
                    if name:
                        item.join_item_name(name)
                        ready_to_send += item.serialize()
                    else:
                        self.pending_items.append(item)

                if ready_to_send:
                    logging.info("READY TO SEND")
                    message.update_content(ready_to_send)
                    serialized = message.serialize()
                    self.data_out_queue.send(serialized)
                    logging.info(f"Sending message: {serialized}")

        except Exception as e:
            logging.error(f"Error en JoinerMenuItems: {e}")

        finally:
            with self.processing_data.get_lock():
                self.processing_data.value = False

            with self.eof_received.get_lock():
                if self.eof_received.value:
                    self.__received_EOF__(Message(0, MESSAGE_TYPE_EOF, 0, 0))

    def __received_EOF__(self, message):
        for queue in self.eof_out_queues:
            queue.send(message.serialize())
            logging.info(f"EOF enviado a réplica | request_id: {message.request_id} | type: {message.type}")

    def _process_pending(self):
        ready_to_send = ''
        for item in list(self.pending_items):
            name = self.menu_items.get(parse_int(item.item_data))
            if name:
                item.join_item_name(name)
                ready_to_send += item.serialize()
                self.pending_items.remove(item)

        if ready_to_send:
            serialized = Message(0, MESSAGE_TYPE_QUERY_4_RESULT, 0, ready_to_send).serialize()
            self.out_queue.send(serialized)
            logging.info(f"Sending message: {serialized}")

    def _on_eof_from_queue_message(self, message):
        try:
            logging.info("Procesando mensaje de EOF queue")
            message = Message.deserialize(message)
        
            with self.leader.get_lock():
                is_leader = self.leader.value

            if is_leader:
                with self.expected_acks.get_lock():
                    if self.expected_acks.value > 0:
                       self.expected_acks.value -= 1
                    if self.expected_acks.value <= 0:
                        self._send_final_eof(message)
            else:
                with self.processing_data.get_lock(), self.eof_received.get_lock():
                    if message.type == MESSAGE_TYPE_EOF and not self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        self.__received_EOF__(message)
                        logging.info("Procesamiento finalizado")
                        return
                    elif message.type == MESSAGE_TYPE_EOF and self.processing_data.value and not self.eof_received.value:
                        self.eof_received.value = True
                        return
                    
        except Exception as e:
            print(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

    def _send_final_eof(self, message):
        self.data_out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            for in_queue in self.data_in_queue:
                in_queue.close()
            for out_queue in self.data_out_queue:
                out_queue.close()
            self.data_in_queue.close()
            self.data_out_queue.close()
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
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["expected_acks"] = int(os.getenv('EXPECTED_ACKS'))
    
    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"], config_params["expected_acks"]]:
        raise ValueError("Expected value not found. Aborting.")
    
    return config_params

def create_eofs_queues(rabbitmq_host):
    input_queues = []
    output_queues = []

    for key, queue_name in os.environ.items():
        if key.startswith("INPUT_QUEUE_"):
            if queue_name.startswith("EOF"):
                input_queues.append(MessageMiddlewareQueue(rabbitmq_host, queue_name))
        elif key.startswith("OUTPUT_QUEUE_"):
            if queue_name.startswith("EOF"):
                output_queues.append(MessageMiddlewareQueue(rabbitmq_host, queue_name))

    return input_queues, output_queues

def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])
    
    eof_input_queues, eof_output_queues = create_eofs_queues(config_params["rabbitmq_host"])

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    menu_items_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_2"])
    data_output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    
    aggregator = JoinerMenuItems(config_params["expected_acks"], data_input_queue, menu_items_input_queue, data_output_queue, eof_input_queues, eof_output_queues)
    aggregator.start()

if __name__ == "__main__":
    main()
