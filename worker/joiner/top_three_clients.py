from worker.joiner.joiner import Joiner 
from Middleware.middleware import MessageMiddlewareQueue
import logging
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_EOF, MESSAGE_TYPE_USERS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT
from utils.custom_logging import initialize_log
import os
from pkg.storage.state_storage.top_three_clients import TopThreeClientsStateStorage
from pkg.message.q4_result import Q4IntermediateResult
import threading

EXPECTED_EOFS = 3 # 1 users, 2 store agg
SNAPSHOT_INTERVAL = 200  # Guardar snapshot cada N mensajes (máximo entre batches)
BATCH_ACK_PREFETCH = 250  # Prefetch for batch ACK to avoid queue blocking

class TopThreeClientsJoiner(Joiner):

    def __init__(self, 
                 data_input_queue: str,
                 data_output_queue: str,
                 users_input_queue: str,
                 host: str,
                 storage_dir: str,
                 container_name: str = None):
        
        super().__init_client_handler__(users_input_queue, host, EXPECTED_EOFS)
        self.data_input_queue = data_input_queue
        self.data_output_queue = data_output_queue
        self.state_storage = TopThreeClientsStateStorage(storage_dir)
        # Sequencing / dedup state (unified)
        self._sender_lock = threading.Lock()
        self._last_msg_by_sender = {}
        
        # Message numbering based on replica ID
        try:
            self.node_id = int(container_name.split('-')[-1]) if container_name else 1
        except (ValueError, AttributeError, IndexError):
            self.node_id = 1
            logging.error(f"Could not parse node_id from {container_name}, defaulting to 1")
        
        self.msg_num_counter = 0
        
        # Manual ACK tracking for batch users
        self.users_ack_pending = []  # (ch, method) tuples to ACK after save
        self.users_count_since_save = {}  # Per request tracking
        self.global_users_count = 0  # Global counter for batch ACK trigger
        
        # Track EOF forwarding to avoid duplicates
        self.eofs_sent = set()

    def start(self):
        # Load persisted state once on startup and hydrate last-msg map
        self.state_storage.load_state_all()
        for _rid, st in self.state_storage.data_by_request.items():
            for sid, num in st.get("last_msg_by_sender", {}).items():
                self._last_msg_by_sender[sid] = max(self._last_msg_by_sender.get(sid, -1), num)
        logging.info(f"action: top_three_clients_starting | items_input_queue: {self.items_input_queue}")
        super().start()

    def _consume_items_to_join_queue(self):
        """Override to use manual ACK for batch user messages."""
        logging.info(f"_consume_items_to_join_queue called with queue: {self.items_input_queue}")
        items_input_queue = MessageMiddlewareQueue(self.items_input_queue, self.connection)
        self.message_middlewares.append(items_input_queue)
        
        def __on_items_message__(msg_body, ch, method):
            try:
                message = Message.deserialize(msg_body)
                logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
                 
                if message.type == MESSAGE_TYPE_EOF:
                    # EOF: ACK immediately (no batch pending)
                    self._ack_pending_users()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logging.info(f"action: eof_received_and_acked | request_id: {message.request_id}")
                    return self._process_on_eof_message__(message)
                
                self._ensure_request(message.request_id)
                self._inc_inflight(message.request_id)
                try:
                    # Track for manual ACK
                    self.users_ack_pending.append((ch, method))
                    self._process_items_to_join(message)
                    
                    # Check if we should save and ACK batch (using global counter)
                    self.users_count_since_save[message.request_id] = self.users_count_since_save.get(message.request_id, 0) + 1
                    self.global_users_count += 1
                    logging.debug(f"action: user_message_received | request_id: {message.request_id} | global_count: {self.global_users_count}")
                    if self.global_users_count >= SNAPSHOT_INTERVAL:
                        self._ack_pending_users()
                        self.users_count_since_save.clear()
                        self.global_users_count = 0
                        logging.info(f"action: users_batch_acked | batch_size: {SNAPSHOT_INTERVAL} | pending_acks_after: {len(self.users_ack_pending)}")
                        
                except Exception as e:
                    logging.error(f"Error procesando mensaje de usuarios: {type(e).__name__}: {e}", exc_info=True)
                    # NACK on error - message returns to queue
                    if self.users_ack_pending and self.users_ack_pending[-1] == (ch, method):
                        self.users_ack_pending.pop()  # Remove the ACK we added
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                finally:
                    if message.type != MESSAGE_TYPE_EOF:
                        self._dec_inflight(message.request_id)
            except Exception as e:
                logging.error(f"Fatal error in items_to_join callback: {type(e).__name__}: {e}", exc_info=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        logging.info(f"Starting items_to_join queue consumption with manual ACK")
        # Use new batch ACK method with optimized prefetch
        items_input_queue.start_consuming_with_batch_ack(__on_items_message__, prefetch_count=BATCH_ACK_PREFETCH)

    def _ack_pending_users(self):
        """Save to disk and flush all pending ACKs after batch."""
        if not self.users_ack_pending:
            logging.debug(f"action: no_pending_users_to_ack")
            return
        
        pending_count = len(self.users_ack_pending)
        logging.info(f"action: batch_ack_starting | pending_count: {pending_count}")
        
        # Guardar a disco ANTES de ACK
        try:
            for request_id in list(self.users_count_since_save.keys()):
                self.state_storage.save_state(request_id, reset_state=False)
            logging.info(f"action: state_saved_to_disk | request_ids: {len(self.users_count_since_save)}")
        except Exception as e:
            logging.error(f"Error saving state to disk: {type(e).__name__}: {e}", exc_info=True)
            return
        
        # Ahora ACK todos los mensajes
        acked_count = 0
        for ch, method in self.users_ack_pending:
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                acked_count += 1
            except Exception as e:
                logging.error(f"Error acking message: {type(e).__name__}: {e}")
        
        self.users_ack_pending = []
        logging.info(f"action: batch_acked_complete | acked_count: {acked_count}")

    def _consume_data_queue(self):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)
        self.message_middlewares.append(data_input_queue)
        
        def __on_message__(msg):
            message = Message.deserialize(msg)
            logging.info(f"action: message received | request_id: {message.request_id} | type: {message.type}")

            if message.type == MESSAGE_TYPE_EOF:
                return self._process_on_eof_message__(message)

            self._ensure_request(message.request_id)
            self._inc_inflight(message.request_id)
            try:
                # Solo procesar TRANSACTIONS aquí
                # Los USERS llegan por el otro queue (_process_items_to_join)
                if message.type != MESSAGE_TYPE_TRANSACTIONS:
                    return
                
                # Dedup/ordering check
                sender_id = message.get_node_id_and_request_id()
                with self._sender_lock:
                    last = self._last_msg_by_sender.get(sender_id, -1)
                    if message.msg_num <= last:
                        if message.msg_num == last:
                            logging.info(f"action: duplicate_msg | sender:{sender_id} | msg:{message.msg_num}")
                        else:
                            logging.warning(f"action: out_of_order_msg | sender:{sender_id} | msg:{message.msg_num} < last:{last}")
                        return
                    self._last_msg_by_sender[sender_id] = message.msg_num
                
                items = message.process_message()
                self._accumulate_transactions(items, message.request_id, sender_id, message.msg_num)
            finally:
                if message.type == MESSAGE_TYPE_TRANSACTIONS:
                    self._dec_inflight(message.request_id)
                
        data_input_queue.start_consuming(__on_message__)
        
    def _accumulate_transactions(self, items, request_id, sender_id=None, msg_num=None):
        """Acumula transacciones por tienda y usuario (guardando después de cada batch)."""
        with self.state_storage._lock:
            state = self.state_storage.data_by_request.setdefault(request_id, {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender": {},
            })
            users_by_store = state["users_by_store"]

            for item in items:
                user_id = item.get_user()
                if not user_id:
                    continue

                store_id = item.get_store()
                if store_id is None:
                    continue
                
                store_users = users_by_store.setdefault(store_id, {})
                store_users[user_id] = store_users.get(user_id, 0) + 1

            # Actualizar marcador de último mensaje
            if sender_id is not None and msg_num is not None:
                state["last_msg_by_sender"][sender_id] = msg_num
        
        # Guardar SIEMPRE después de procesar batch (tolerante a fallos)
        self.state_storage.save_state(request_id, reset_state=False)
        logging.debug(f"action: transactions_accumulated | request_id: {request_id} | items: {len(items)}")

    def _accumulate_users(self, items, request_id, sender_id=None, msg_num=None):
        """Acumula información de usuarios (guardando después de cada batch)."""
        with self.state_storage._lock:
            state = self.state_storage.data_by_request.setdefault(request_id, {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender": {},
            })
            users_birthdates = state["users_birthdates"]

            for item in items:
                user_id = item.get_user_id()
                birthdate = item.get_birthdate()
                if user_id and birthdate:
                    users_birthdates[user_id] = birthdate

            # Actualizar marcador de último mensaje
            if sender_id is not None and msg_num is not None:
                state["last_msg_by_sender"][sender_id] = msg_num
        
        # Guardar SIEMPRE después de procesar batch (tolerante a fallos)
        self.state_storage.save_state(request_id, reset_state=False)
        logging.debug(f"action: users_accumulated | request_id: {request_id} | items: {len(items)}")

    def _process_items_to_join(self, message):
        """Process user items from the users queue (NO save here - done in batch)."""
        # This is called by the base Joiner class when processing the users_input_queue
        sender_id = message.get_node_id_and_request_id()
        with self._sender_lock:
            last = self._last_msg_by_sender.get(sender_id, -1)
            if message.msg_num <= last:
                if message.msg_num == last:
                    logging.info(f"action: duplicate_msg | stream:users | sender:{sender_id} | msg:{message.msg_num}")
                else:
                    logging.warning(f"action: out_of_order_msg | stream:users | sender:{sender_id} | msg:{message.msg_num} < last:{last}")
                return
            self._last_msg_by_sender[sender_id] = message.msg_num
        
        items = message.process_message()
        # Acumular usuarios directamente en memoria (NO guardar aquí)
        with self.state_storage._lock:
            state = self.state_storage.data_by_request.setdefault(message.request_id, {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender": {},
            })
            users_birthdates = state["users_birthdates"]

            for item in items:
                user_id = item.get_user_id()
                birthdate = item.get_birthdate()
                if user_id and birthdate:
                    users_birthdates[user_id] = birthdate

            # Actualizar marcador de último mensaje
            state["last_msg_by_sender"][sender_id] = message.msg_num
        
        logging.debug(f"action: users_accumulated | request_id: {message.request_id} | items: {len(items)}")

    def _process_on_eof_message__(self, message):
        """Handle EOF message: delegates to parent's method which calls _send_results."""
        return super()._process_on_eof_message__(message)
        
    def _send_results(self, message):
        data_output_queue = MessageMiddlewareQueue(self.data_output_queue, self.connection)
        self.message_middlewares.append(data_output_queue)
        
        # Force save any pending users before EOF (in case batch threshold not reached)
        if self.global_users_count > 0:
            logging.info(f"action: flushing_pending_users_before_eof | request_id: {message.request_id} | pending_count: {self.global_users_count}")
            self._ack_pending_users()
        
        # Reload from disk to ensure consistency before sending results
        # (en caso de que haya habido un issue con la memoria)
        self.state_storage.load_state(message.request_id)
        
        self._process_top_3_by_request(message.request_id, data_output_queue)
        self._send_eof(message, data_output_queue)
        
        # Persist state to disk ONLY at the end (atomically)
        self.state_storage.save_state(message.request_id)
        self.state_storage.delete_state(message.request_id)
        
    def _process_top_3_by_request(self, request_id, data_output_queue):
        """Send 1 message per store with top-3 users joined with birthdates."""
        saved_state = self.state_storage.data_by_request.get(request_id, {})
        users_by_store_state = saved_state.get("users_by_store", {})
        users_birthdates_state = saved_state.get("users_birthdates", {})
        
        stores_sent = 0

        for store_id, users in users_by_store_state.items():
            if store_id is None:
                continue

            # users: dict user_id -> count
            sorted_users = sorted(users.items(), key=lambda x: (-x[1], x[0]))

            unique_values = []
            for user_id, count in sorted_users:
                if count not in unique_values:
                    unique_values.append(count)
                if len(unique_values) == 3:
                    break

            top_3_users = [user for user in sorted_users if user[1] in unique_values]

            # Construir un SOLO chunk/mensaje para este store con todos sus top-3 usuarios
            chunk = ""
            for user_id, transaction_count in top_3_users:
                birthdate = users_birthdates_state.get(user_id)
                if birthdate:
                    res = Q4IntermediateResult(store_id, birthdate, transaction_count)
                    chunk += res.serialize()
            
            # Enviar UN SOLO mensaje por store con todos sus top-3 usuarios
            if chunk:
                new_msg_num = self.msg_num_counter
                self.msg_num_counter += 1
                new_message = Message(request_id, MESSAGE_TYPE_QUERY_4_INTERMEDIATE_RESULT, new_msg_num, chunk)
                new_message.add_node_id(self.node_id)
                data_output_queue.send(new_message.serialize())
                stores_sent += 1
                logging.debug(f"action: store_result_sent | request_id: {request_id} | store_id: {store_id} | top_3_count: {len(top_3_users)}")
        
        logging.info(f"action: send_results_done | request_id: {request_id} | stores_sent: {stores_sent}")
        
    def _send_eof(self, message, data_output_queue):
        """Forward EOF downstream - ensure only 1 EOF per request_id."""
        if message.request_id in self.eofs_sent:
            logging.warning(f"action: eof_already_sent | request_id: {message.request_id} | node_id: {self.node_id}")
            return
        
        self.eofs_sent.add(message.request_id)
        data_output_queue.send(message.serialize())
        logging.info(f"action: eof_forwarded | request_id: {message.request_id} | node_id: {self.node_id}")


def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_queue"] = os.getenv('OUTPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', './data')
    config_params["container_name"] = os.getenv('CONTAINER_NAME', 'top-three-clients')

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"], config_params["output_queue"]]:
        raise ValueError("Expected value not found. Aborting.")

    return config_params



def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    joiner = TopThreeClientsJoiner(config_params["input_queue_1"], 
                                   config_params["output_queue"], 
                                   config_params["input_queue_2"], 
                                   config_params["rabbitmq_host"],
                                   config_params["storage_dir"],
                                   config_params["container_name"])
    joiner.start()


if __name__ == "__main__":
    main()
