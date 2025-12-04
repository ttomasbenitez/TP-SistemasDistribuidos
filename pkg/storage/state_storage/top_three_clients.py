from pkg.storage.state_storage.base import StateStorage

class TopThreeClientsStateStorage(StateStorage):
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            TR;store_id;user_id;count
            UB;user_id;birthdate

        Estructura de destino:
            self.data_by_request[request_id] = {
                "users_by_store": { store_id: { user_id: total_count } },
                "users_birthdates": { user_id: birthdate },
                "last_by_sender": { `sender_id:steam` : last_num_sent }
            }
        """

        state = self.data_by_request.setdefault(request_id, {
            "users_by_store": {},
            "users_birthdates": {},
            "last_by_sender": {}
        })

        users_by_store = state["users_by_store"]
        users_birthdates = state["users_birthdates"]
        last_by_sender = state["last_by_sender"]

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            kind = parts[0]

            if kind == "TR":
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

            if kind == "UB":
                _k, user_id_str, birthdate = parts
                try:
                    user_id = int(user_id_str)
                except ValueError as e:
                    continue

                users_birthdates[user_id] = birthdate
                continue
            
            if kind == "SE":
                _k, sender_key, last_str = parts

                try:
                    last_num = int(last_str)
                except ValueError as e:
                    continue

                last_by_sender[sender_key] = max(last_by_sender.get(sender_key, -1), last_num)
                continue
            
        self.data_by_request[request_id]["users_by_store"] = users_by_store 
        self.data_by_request[request_id]["users_birthdates"] = users_birthdates
        self.data_by_request[request_id]["last_by_sender"] = last_by_sender
        
            
    def _append_transaction(self, users_by_store, file_handle):
        for store_id, users in users_by_store.items():
            for user_id, count in users.items():
                line = f"TR;{store_id};{user_id};{count}\n"
                file_handle.write(line)
                
    def _append_birthdates(self, users_birthdates, file_handle):
        for user_id, birthdate in users_birthdates.items():
            line = f"UB;{user_id};{birthdate}\n"
            file_handle.write(line)
            
    def _append_sender_data(self, last_by_sender, file_handle):
        for sender_id, last_num in last_by_sender.items():
            line = f"SE;{sender_id};{last_num}\n"
            file_handle.write(line)
                
    
    def _save_state_to_file(self, file_handle, request_id):
        state = self.data_by_request.get(request_id)
        if not state:
            return

        users_by_store = state.get("users_by_store", {})
        users_birthdates = state.get("users_birthdates", {})
        last_data = state.get("last_by_sender", {})
        
        self._append_transaction(users_by_store, file_handle)
        self._append_birthdates(users_birthdates, file_handle)
        self._append_sender_data(last_data, file_handle)
        