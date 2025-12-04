from pkg.storage.state_storage.base import StateStorage
from pkg.message.q2_result import Q2Result

class JoinerMenuItemsStateStorage(StateStorage):
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            M;menu_item_id;name
            S;sender_id;last_num_sent
            PC;serialized_client_data  # Q2Result

        Estructura de destino:
            self.data_by_request[request_id] = {
                "menu_items": { menu_item_id: name },
                "last_by_sender": { sender_id: last_num_sent },
                "pending_results": [Q2Result],
            }
        """

        state = self.data_by_request.setdefault(request_id, {
            "menu_items": {},
            "last_by_sender": {},
            "pending_results": [],
        })

        menu_items = state["menu_items"]
        last_by_sender = state["last_by_sender"]
        pending_results = state["pending_results"]

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")

            kind = parts[0]

            if kind == "ME":
                _k, menu_item_id_str, menu_item_name = parts
                try:
                    menu_item_id = int(menu_item_id_str)
                except ValueError as e:
                    continue

                menu_items[menu_item_id] = menu_item_name
                continue

            if kind == "SE":
                _k, sender_str, last_str = parts

                try:
                    sender_id = int(sender_str)
                    last_num = int(last_str)
                except ValueError as e:
                    continue

                last_by_sender[sender_id] = max(last_by_sender.get(sender_id, -1), last_num)
                continue
            
            if kind == "PC":
                _k, year_month_created_at, item_data, value, result_type = parts
                pending_results.append(Q2Result(year_month_created_at, item_data, value, result_type))
            
    def _append_sender_data(self, last_by_sender, file_handle):
        for sender_id, last_num in last_by_sender.items():
            line = f"SE;{sender_id};{last_num}\n"
            file_handle.write(line)
                
    def _append_menu_items(self, menu_items, file_handle):
        for menu_items_id, name in menu_items.items():
            line = f"ME;{menu_items_id};{name}\n"
            file_handle.write(line)
    
    def _append_pending_results(self, pending_results, file_handle):
        for q2_result in pending_results:
            serialialized = q2_result.serialize()
            line = f"PC;{serialialized}"
            file_handle.write(line)
    
    def _save_state_to_file(self, file_handle, request_id):
        state = self.data_by_request.get(request_id)
        if not state:
            return

        self._append_sender_data(state["last_by_sender"], file_handle)
        self._append_menu_items(state["menu_items"], file_handle)
        self._append_pending_results(state["pending_results"], file_handle)
        