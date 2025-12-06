from pkg.storage.state_storage.base import StateStorage
import logging
import os
import threading


class TopThreeClientsStateStorage(StateStorage):
    """
    Persist user transaction counts per store and user birthdates.
    File format (one record per line):
      - T;{store_id};{user_id};{count}   # Transaction count for user at store
      - B;{user_id};{birthdate}          # User birthdate
      - S;{sender_id};{last_msg_num}     # Last message number for dedup (unified senders)
    
    Persistence strategy: NO incremental writes. Keep everything in memory during request
    processing, then save atomically to disk only when EOF is received.
    """

    def __init__(self, default_state):
        """
        In-memory only storage for TopThreeClients. No disk I/O.
        """
        # Do NOT call super().__init__ to avoid creating directories
        self.data_by_request = {}
        self._data_lock = threading.Lock()
        self.default_state = default_state
        # storage_dir intentionally omitted (no persistence)

    # ---- Disable disk I/O entirely (no-ops) ----
    def load_state_all(self):
        return

    def load_state(self, request_id):
        return

    def save_state(self, request_id):
        return

    def append_state(self, request_id):
        return

    def delete_state(self, request_id):
        # Only clear in-memory state
        with self._data_lock:
            if request_id in self.data_by_request:
                del self.data_by_request[request_id]

    # ---- Legacy file handlers kept for compatibility but unused ----
    def _load_state_from_file(self, file_handle, request_id):
        """
        Load persisted state from file.
        Formats:
          T;store_id;user_id;count       # transactions
          B;user_id;birthdate             # birthdates
          S;sender_id;last_msg_num        # sender last-seen (unified)
        """
        users_by_store = {}
        users_birthdates = {}
        last_by_sender = {}
        
        state = self.data_by_request.setdefault(request_id, {
            "users_by_store": {},
            "last_by_sender": {},
            "users_birthdates": [],
            "last_eof_count": 0,
        })
        
        loaded_transactions = 0
        loaded_birthdates = 0
        loaded_senders = 0

        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            
            try:
                parts = line.split(";")
                if len(parts) < 2:
                    continue
                    
                kind = parts[0]
                
                if kind == "T" and len(parts) == 4:
                    # Transaction: T;store_id;user_id;count
                    _, store_id_str, user_id_str, count_str = parts
                    try:
                        store_id = int(store_id_str)
                        user_id = int(user_id_str)
                        count = int(count_str)
                        
                        store_users = users_by_store.setdefault(store_id, {})
                        store_users[user_id] = store_users.get(user_id, 0) + count
                        loaded_transactions += 1
                        logging.debug(f"action: loaded_transaction | request_id: {request_id} | store_id: {store_id} | user_id: {user_id} | count: {count}")
                    except (ValueError, IndexError) as e:
                        logging.warning(f"action: load_transaction_error | request_id: {request_id} | line: {line} | error: {e}")
                        continue
                
                elif kind == "B" and len(parts) == 3:
                    # Birthdate: B;user_id;birthdate
                    _, user_id_str, birthdate = parts
                    try:
                        user_id = int(user_id_str)
                        users_birthdates[user_id] = birthdate
                        loaded_birthdates += 1
                        logging.debug(f"action: loaded_birthdate | request_id: {request_id} | user_id: {user_id}")
                    except (ValueError, IndexError) as e:
                        logging.warning(f"action: load_birthdate_error | request_id: {request_id} | line: {line} | error: {e}")
                        continue
                
                elif kind == "S" and len(parts) == 3:
                    # Sender: S;sender_id;last_msg_num (unified)
                    _, sender_id, last_str = parts
                    try:
                        last = int(last_str)
                        last_by_sender[sender_id] = max(last_by_sender.get(sender_id, -1), last)
                        loaded_senders += 1
                        logging.debug(f"action: loaded_sender | request_id: {request_id} | sender: {sender_id} | last_msg: {last}")
                    except (ValueError, IndexError) as e:
                        logging.warning(f"action: load_sender_error | request_id: {request_id} | line: {line} | error: {e}")
                        continue   
                
                if kind == "LE":
                    _k, last_eof_str = parts
                    try:
                        last_eof = int(last_eof_str)
                    except ValueError as e:
                        continue
                    last_eof_count = max(state["last_eof_count"], last_eof)
                    continue

            except Exception as e:
                logging.warning(f"action: state_parse_error | request_id: {request_id} | line: {line} | error: {e}")
                continue
        
        # Store in data_by_request
        self.data_by_request.setdefault(request_id, {})
        self.data_by_request[request_id]["users_by_store"] = users_by_store
        self.data_by_request[request_id]["users_birthdates"] = users_birthdates
        self.data_by_request[request_id]["last_by_sender"] = last_by_sender
        self.data_by_request[request_id]["last_eof_count"] = last_eof_count
        
        logging.info(f"action: state_loaded | request_id: {request_id} | transactions: {loaded_transactions} | birthdates: {loaded_birthdates} | senders: {loaded_senders}")

    def _save_state_to_file(self, file_handle, request_id):
        """
        Writes to file using the following format per line:
          T;store_id;user_id;count
          B;user_id;birthdate
          S;sender_id;last_msg_num
        
        The caller sets state["users_by_store"], state["users_birthdates"], and
        state["last_by_sender"] during accumulation and we write them
        incrementally to disk.
        """
        state = self.data_by_request.get(request_id, {})
        
        # Write transaction counts
        users_by_store = state.get("users_by_store", {})
        for store_id, users in users_by_store.items():
            for user_id, count in users.items():
                line = f"T;{store_id};{user_id};{count}\n"
                file_handle.write(line)
        
        # Write birthdates
        users_birthdates = state.get("users_birthdates", {})
        for user_id, birthdate in users_birthdates.items():
            line = f"B;{user_id};{birthdate}\n"
            file_handle.write(line)
        
        # Write sender last-msg markers (unified)
        last_by_sender = state.get("last_by_sender", {})
        for sender_id, last_msg in last_by_sender.items():
            line = f"S;{sender_id};{last_msg}\n"
            file_handle.write(line)

        # Print aproximado de lo que se va a guardar (muestras pequeñas para no saturar salida)
        sample_users_by_store = {sid: list(users.keys())[:3] for sid, users in list(users_by_store.items())[:1]}
        sample_birthdates = list(users_birthdates.items())[:3]
        sample_senders = list(last_by_sender.items())[:3]
        print(f"[TopThreeClientsStateStorage] Persistiendo estado | request_id={request_id} | "
              f"stores={len(users_by_store)} | birthdates={len(users_birthdates)} | senders={len(last_by_sender)} | "
              f"samples -> stores:{sample_users_by_store} birthdates:{sample_birthdates} senders:{sample_senders}")
        
        logging.debug(f"action: state_written | request_id: {request_id} | stores: {len(users_by_store)} | birthdates: {len(users_birthdates)} | senders: {len(last_by_sender)}")

    def append_user_birthdates(self, request_id, user_birthdates_delta):
        """DEPRECATED: Not used anymore. Kept for backwards compatibility."""
        pass

    def append_sender_marker(self, request_id, sender_id, last_msg_num):
        """DEPRECATED: Not used anymore. Kept for backwards compatibility."""
        pass