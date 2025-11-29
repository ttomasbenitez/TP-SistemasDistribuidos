from pkg.storage.state_storage.base import StateStorage

class TopThreeClientsStateStorage(StateStorage):
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            T;store_id;user_id;count
            U;user_id;birthdate

        Estructura de destino:
            self.data_by_request[request_id] = {
                "users_by_store": { store_id: { user_id: total_count } },
                "users_birthdates": { user_id: birthdate }
            }
        """

        state = self.data_by_request.setdefault(request_id, {
            "users_by_store": {},
            "users_birthdates": {}
        })

        users_by_store = state["users_by_store"]
        users_birthdates = state["users_birthdates"]

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
                except ValueError as e:
                    continue

                store_users = users_by_store.setdefault(store_id, {})
                store_users[user_id] = store_users.get(user_id, 0) + count
                continue

            if kind == "B":
                _k, user_id_str, birthdate = parts

                try:
                    user_id = int(user_id_str)
                except ValueError as e:
                    continue

                users_birthdates[user_id] = birthdate
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

        users_by_store = state["users_by_store"]
        users_birthdates = state["users_birthdates"]
        
        self._append_transaction(users_by_store, file_handle)
        self._append_birthdates(users_birthdates, file_handle)
        