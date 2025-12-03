from pkg.storage.state_storage.base import StateStorage

class TopThreeClientsStateStorage(StateStorage):
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            T;store_id;user_id;count           # acumulación de transacciones por tienda/usuario
            B;user_id;birthdate                # catálogo de usuarios (birthdates)
            SD;sender_id;last_msg_num          # último msg de stream de datos por emisor
            SU;sender_id;last_msg_num          # último msg de stream de usuarios por emisor

        Estructura de destino:
            self.data_by_request[request_id] = {
                "users_by_store": { store_id: { user_id: total_count } },
                "users_birthdates": { user_id: birthdate },
                "last_msg_by_sender_data": { sender_id: last_msg_num },
                "last_msg_by_sender_users": { sender_id: last_msg_num }
            }
        """

        state = self.data_by_request.setdefault(
            request_id,
            {
                "users_by_store": {},
                "users_birthdates": {},
                "last_msg_by_sender_data": {},
                "last_msg_by_sender_users": {},
            },
        )

        users_by_store = state["users_by_store"]
        users_birthdates = state["users_birthdates"]
        last_data = state["last_msg_by_sender_data"]
        last_users = state["last_msg_by_sender_users"]

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            kind = parts[0]

            if kind == "T":
                _k, store_id_str, user_id_str, count_str = parts
                try:
                    store_id = int(store_id_str)
                    user_id = int(user_id_str)
                    count = int(count_str)
                except ValueError:
                    continue
                store_users = users_by_store.setdefault(store_id, {})
                store_users[user_id] = store_users.get(user_id, 0) + count
                continue

            if kind == "B":
                _k, user_id_str, birthdate = parts
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    continue
                users_birthdates[user_id] = birthdate
                continue

            if kind == "SD":
                _k, sender_id, last_str = parts
                try:
                    last = int(last_str)
                except ValueError:
                    continue
                last_data[sender_id] = max(last_data.get(sender_id, -1), last)
                continue

            if kind == "SU":
                _k, sender_id, last_str = parts
                try:
                    last = int(last_str)
                except ValueError:
                    continue
                last_users[sender_id] = max(last_users.get(sender_id, -1), last)
                continue
    
    def _append_transaction(self, users_by_store, file_handle):
        for store_id, users in users_by_store.items():
            for user_id, count in users.items():
                line = f"T;{store_id};{user_id};{count}\n"
                file_handle.write(line)
                
    def _append_birthdates(self, users_birthdates, file_handle):
        for user_id, birthdate in users_birthdates.items():
            line = f"B;{user_id};{birthdate}\n"
            file_handle.write(line)
    
    def _save_state_to_file(self, file_handle, request_id):
        state = self.data_by_request.get(request_id)
        if not state:
            return

        users_by_store = state.get("users_by_store", {})
        users_birthdates = state.get("users_birthdates", {})
        last_data = state.get("last_msg_by_sender_data", {})
        last_users = state.get("last_msg_by_sender_users", {})
        
        self._append_transaction(users_by_store, file_handle)
        self._append_birthdates(users_birthdates, file_handle)
        # persist marks of last seen message per sender
        for sid, num in last_data.items():
            file_handle.write(f"SD;{sid};{num}\n")
        for sid, num in last_users.items():
            file_handle.write(f"SU;{sid};{num}\n")
        