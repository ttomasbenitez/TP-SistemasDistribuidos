from worker.base import Worker 
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.message.q4_result import Q4IntermediateResult

EXPECTED_EOFS = 2

class TopThreeClientsJoiner(Worker):
    """
    Wrapper para manejar menu_items y procesar mensajes de varias colas en procesos separados
    """

    def __init__(self, input_exchange: MessageMiddlewareExchange, out_queue: MessageMiddlewareQueue):
        super().__init__(input_exchange)
        self.out_queue = out_queue
        self.eof_count = 0
        self.users = dict() 
        self.users_by_store = dict()  
        self.eofs_by_client = {}

    def __on_message__(self, msg):
        message = Message.deserialize(msg)
        if message.type == MESSAGE_TYPE_EOF:
            self.eofs_by_client[message.request_id] = self.eofs_by_client.get(message.request_id, 0) + 1
            if self.eofs_by_client[message.request_id] < EXPECTED_EOFS:
                logging.info(f"EOF recibido {self.eofs_by_client[message.request_id]}/{EXPECTED_EOFS} | request_id: {message.request_id} | type: {message.type}")
                return
            
            self._process_top_3_by_request(message.request_id)
            self._send_eof(message)
            return

        items = message.process_message()

        if message.type == MESSAGE_TYPE_USERS:
            for item in items:
                key = (item.get_user_id(), message.request_id)
                self.users[key] = item.get_birthdate()
        
        elif message.type == MESSAGE_TYPE_TRANSACTIONS:
            pre_process = dict()
            store_id = None
            for item in items:
                key = (item.get_user_id(), message.request_id)
                pre_process[key] = pre_process.get(key, 0) + 1
                if store_id is None:
                    store_id = item.get_store() 
            if (store_id, message.request_id) not in self.users_by_store:
                self.users_by_store[(store_id, message.request_id)] = dict()
                
            for user_id, count in pre_process.items():
                if user_id:
                    self.users_by_store[(store_id, message.request_id)][user_id] = self.users_by_store[(store_id, message.request_id)].get(user_id, 0) + count


    def _process_top_3_by_request(self, request_id):
        for (store, req_id), users in self.users_by_store.items():
            if store is None:
                continue
            
            if req_id != request_id:
                continue

            unique_values = []

            sorted_users = sorted(users.items(), key=lambda x: (-x[1], x[0]))
 
            for user in sorted_users:
                if user[1] not in unique_values:
                    unique_values.append(user[1])
                if len(unique_values) == 3:
                    break

            top_3_users = [user for user in sorted_users if user[1] in unique_values]
            chunk = ''
            for user in top_3_users:
                user_id, transaction_count = user
                birthdate = self.users.get(user_id, 'N/A')
                chunk += Q4IntermediateResult(store, birthdate, transaction_count).serialize()
            
            msg = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, 1, chunk)
            self.out_queue.send(msg.serialize())

# Filtrar los usuarios que tienen valores en los top 3
            

    def _send_eof(self, message):
        self.out_queue.send(message.serialize())
        logging.info(f"EOF enviado | request_id: {message.request_id} | type: {message.type}")

    def close(self):
        try:
            self.out_queue.close()
            self.in_middleware.close()
        except Exception as e:
            logging.error(f"Error al cerrar: {type(e).__name__}: {e}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["input_exchange"] = os.getenv('EXCHANGE_NAME')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue"])
    input_exchange = MessageMiddlewareExchange(config_params["rabbitmq_host"], config_params["input_exchange"], 
                                        {config_params["input_queue_1"]: [str(MESSAGE_TYPE_TRANSACTIONS), str(MESSAGE_TYPE_EOF)], 
                                        config_params["input_queue_2"]: [str(MESSAGE_TYPE_USERS), str(MESSAGE_TYPE_EOF)]})

    joiner = TopThreeClientsJoiner(input_exchange, output_queue)
    joiner.start()


if __name__ == "__main__":
    main()
