from pkg.storage.state_storage.base import StateStorage
import logging


class TopThreeClientsStateStorage(StateStorage):
    """
    Persiste el estado por request_id:

        {
            "users_by_store": {   # store_id -> user_id -> count
                store_id: {
                    user_id: count
                },
                ...
            },
            "last_by_sender": {   # sender_id -> last_msg_num
                sender_id: last_msg_num,
                ...
            },
        }

    Formato en archivo (por request_id):

        U;store_id;user_id;count
        U;store_id;user_id;count
        ...
        S;sender_id;last_msg_num
        S;sender_id;last_msg_num
        ...
    """

    def __init__(self, storage_dir: str):
        default_state = {
            "users_by_store": {},   # store_id -> user_id -> count
            "last_by_sender": {},   # sender_id -> last_msg_num
        }
        filepath = f"{storage_dir}/top_three_clients_storage"
        super().__init__(filepath, default_state)

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde un archivo específico para un request_id.

        Formatos esperados (una por línea):
            U;store_id;user_id;count
            S;sender_id;last_msg_num
        """
        state = self.data_by_request.setdefault(
            request_id,
            {
                "users_by_store": {},
                "last_by_sender": {},
            },
        )

        users_by_store = {}
        last_by_sender = {}

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            if not parts:
                continue

            record_type = parts[0]

            # -----------------------------
            # U;store_id;user_id;count
            # -----------------------------
            if record_type == "U":
                if len(parts) != 4:
                    logging.warning(
                        f"action: invalid_user_line_format | request_id: {request_id} | line: {line}"
                    )
                    continue

                _, store_id_str, user_id_str, count_str = parts
                try:
                    store_id = int(store_id_str)
                    user_id = int(user_id_str)
                    count = int(count_str)
                except ValueError as e:
                    logging.warning(
                        f"action: invalid_user_numbers | request_id: {request_id} "
                        f"| line: {line} | error: {e}"
                    )
                    continue

                store_users = users_by_store.setdefault(store_id, {})
                # acumulamos por si aparece varias veces el mismo par
                store_users[user_id] = store_users.get(user_id, 0) + count

            # -----------------------------
            # S;sender_id;last_msg_num
            # -----------------------------
            elif record_type == "S":
                if len(parts) != 3:
                    logging.warning(
                        f"action: invalid_sender_line_format | request_id: {request_id} | line: {line}"
                    )
                    continue

                _, sender_id, last_num_str = parts
                try:
                    last_num = int(last_num_str)
                except ValueError as e:
                    logging.warning(
                        f"action: invalid_sender_number | request_id: {request_id} "
                        f"| line: {line} | error: {e}"
                    )
                    continue

                # si aparece varias veces el mismo sender, nos quedamos con el max
                prev = last_by_sender.get(sender_id, -1)
                last_by_sender[sender_id] = max(prev, last_num)

            else:
                logging.warning(
                    f"action: unknown_record_type | request_id: {request_id} "
                    f"| type: {record_type} | line: {line}"
                )
                continue

        state["users_by_store"] = users_by_store
        state["last_by_sender"] = last_by_sender


    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en el archivo específico para un request_id.

        Formato:
            U;store_id;user_id;count
            S;sender_id;last_msg_num
        """
        state = self.get_state(request_id)
        if not state:
            logging.warning(
                f"action: save_state_no_data | request_id: {request_id}"
            )
            return

        users_by_store = state.get("users_by_store", {})
        last_by_sender = state.get("last_by_sender", {})


        # Usuarios por tienda
        for store_id, users_dict in users_by_store.items():
            for user_id, count in users_dict.items():
                line = f"U;{store_id};{user_id};{count}\n"
                file_handle.write(line)

        # Último mensaje por sender
        for sender_id, last_num in last_by_sender.items():
            line = f"S;{sender_id};{last_num}\n"
            file_handle.write(line)

