from worker import Worker 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
from pkg.message.q2_result import Q2IntermediateResult
import os
from multiprocessing import Process, Value


class AggregatorMonth(Worker):
    
    def __init__(self, expected_acks: int, data_in_queue: MessageMiddlewareQueue, data_out_queue: MessageMiddlewareQueue, eof_in_queues: list[MessageMiddlewareQueue], eof_out_queues: list[MessageMiddlewareQueue]):
        self.data_in_queue = data_in_queue
        self.data_out_queue = data_out_queue
        self.eof_out_queues = eof_out_queues
        self.eof_in_queues = eof_in_queues
        self.eof_received = Value('b', False)  # 'b' = boolean
        self.leader = Value('b', expected_acks == 0)
        self.processing_data = Value('b', False)
        self.expected_acks = Value('i', expected_acks)
    
    def start(self):
        # Creamos un proceso por cada input queue
        processes = []

        logging.info(f"Starting process")
        p = Process(target=self._consume_data_queue, args=(self.data_in_queue,))
        p.start()
        processes.append(p)

        for queue in self.eof_in_queues:
            logging.info(f"Starting process")
            p = Process(target=self._consume_eof_queue, args=(queue,))
            p.start()
            processes.append(p)

        # Esperamos que terminen
        for p in processes:
            p.join()

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

    def __on_message__(self, message):
        try:
            message = Message.deserialize(message)

            if message.type == MESSAGE_TYPE_EOF:
                with self.leader.get_lock():
                    if self.leader.value:
                        self._send_final_eof(message)
                    else: 
                        self.leader.value = True
                        self.__received_EOF__(message)
                return

            with self.processing_data.get_lock():
                self.processing_data.value = True

            items = message.process_message()
            groups = self._group_items_by_month(items)
            new_message = Message(message.request_id, MESSAGE_TYPE_QUERY_2_INTERMEDIATE_RESULT, message.msg_num, '')
            self._send_groups(new_message, groups)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {type(e).__name__}: {e}")

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

    def _send_final_eof(self, message):
        self.data_out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def _group_items_by_month(self, items):
        groups = {}
        for item in items:
            month = item.get_month()
            year = item.get_year()
            q4_intermediate = Q2IntermediateResult(f"{year}-{month}", item.item_id, item.quantity, item.subtotal)
            groups.setdefault(f"{year}-{month}", []).append(q4_intermediate)
        return groups
    
    def _send_groups(self, original_message, groups):
        for month, month_items in groups.items():
            new_chunk = ''.join(item.serialize() for item in month_items)
            new_message = original_message.new_from_original(new_chunk)
            serialized = new_message.serialize()
            self.data_out_queue.send(serialized)
            logging.info(f"Agregado correctamente | request_id: {new_message.request_id} | type: {new_message.type} | month: {month}")
    
    def close(self):
        try:
            for in_queue in self.eof_in_queues:
                in_queue.close()
            for out_queue in self.eof_out_queues:
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
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["expected_acks"] = int(os.getenv('EXPECTED_ACKS'))

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None or config_params["output_queue"] is None or config_params["expected_acks"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
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

    data_input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    data_output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    
    aggregator = AggregatorMonth(config_params["expected_acks"], data_input_queue, data_output_queue, eof_input_queues, eof_output_queues)
    aggregator.start()

if __name__ == "__main__":
    main()