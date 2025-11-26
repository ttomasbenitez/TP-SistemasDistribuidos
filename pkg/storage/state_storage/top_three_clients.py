from pkg.storage.state_storage.base import StateStorage

class TopThreeClientsStateStorage(StateStorage):
    
    def _load_state_from_file(self, file_handle, request_id):
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

            store_id_str, user_id_str, count_str, birthdate = line.split(";", 3)
            store_id = int(store_id_str)
            user_id = int(user_id_str)
            count = int(count_str)

            store_users = users_by_store.setdefault(store_id, {})
            store_users[user_id] = store_users.get(user_id, 0) + count

            users_birthdates[user_id] = birthdate
    
    def _save_state_to_file(self, file_handle, request_id):
        state = self.data_by_request.get(request_id)
        if not state:
            return

        users_by_store = state["users_by_store"]
        users_birthdates = state["users_birthdates"]

        for store_id, users in users_by_store.items():
            for user_id, count in users.items():
                birthdate = users_birthdates.get(user_id, "")
                line = f"{store_id};{user_id};{count};{birthdate}\n"
                file_handle.write(line)