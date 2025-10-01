from worker import Worker
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_TRANSACTION_ITEMS
from utils.custom_logging import initialize_log
import os
from multiprocessing import Process, Manager, Value

EXPECTED_EOFS = 2

class JoinerMenuItems:
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, menu_items_input_queue: MessageMiddlewareQueue,
                 products_in_queue: MessageMiddlewareQueue,
                 out_queue: MessageMiddlewareQueue):
        
        self.out_queue = out_queue
        self.manager = Manager()
        self.menu_items = self.manager.dict()  # compartido entre procesos
        self.pending_items = self.manager.list()  # TransactionItems pendientes de join
        self.received_eofs = Value('i', 0)

        # Guardamos referencias a las colas
        self.queues = [
            ("menu_items", menu_items_input_queue),
            ("products", products_in_queue)
        ]

    def start(self):
        # Creamos un proceso por cada cola
        processes = []
        for name, queue in self.queues:
            logging.info(f"Starting process for queue: {name}")
            p = Process(target=self._consume_queue, args=(queue,))
            p.start()
            processes.append(p)

        # Esperamos que terminen
        for p in processes:
            p.join()

    def _consume_queue(self, queue: MessageMiddlewareQueue):
        queue.start_consuming(self._on_message)

    def _on_message(self, message):
        message = Message.read_from_bytes(message)

        if message.type == MESSAGE_TYPE_EOF:
            with self.received_eofs.get_lock():
                self.received_eofs.value += 1
                if self.received_eofs.value == EXPECTED_EOFS:
                    # Procesar pendientes
                    self._process_pending()
                    self._send_eof(message)
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_MENU_ITEMS:
            for item in items:
                self.menu_items[item.get_id()] = item.get_name()

        elif message.type == MESSAGE_TYPE_TRANSACTION_ITEMS:
            ready_to_send = ''
            for item in items:
                name = self.menu_items.get(item.item_id)
                if name:
                    item.item_id = name
                    ready_to_send += item.serialize()
                else:
                    # Guardar para procesar más tarde
                    self.pending_items.append(item)

            if ready_to_send:
                message.update_content(ready_to_send)
                serialized = message.serialize()
                self.out_queue.send(serialized)
                logging.info(f"Sending message: {serialized}")

    def _process_pending(self):
        ready_to_send = ''
        for item in list(self.pending_items):
            name = self.menu_items.get(item.item_id)
            if name:
                item.item_id = name
                ready_to_send += item.serialize()
                self.pending_items.remove(item)

        if ready_to_send:
            serialized = Message(0, MESSAGE_TYPE_TRANSACTION_ITEMS, 0, ready_to_send).serialize()
            self.out_queue.send(serialized)
            logging.info(f"Sending message: {serialized}")

    def _send_eof(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            for _, queue in self.queues:
                queue.close()
            self.out_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_1"])
    menu_items_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue_2"])
    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])

    joiner = JoinerMenuItems(menu_items_input_queue, data_input_queue, output_queue)
    joiner.start()


if __name__ == "__main__":
    main()
