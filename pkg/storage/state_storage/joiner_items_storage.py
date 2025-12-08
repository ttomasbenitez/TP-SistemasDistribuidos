from pkg.storage.state_storage.base import StateStorage
import logging


class JoinerItemsStateStorage(StateStorage):
    
    def __init__(self, storage_dir: str):
        default_state = {
            "items": {},
        }
        filepath = f"{storage_dir}/items_storage"
        super().__init__(filepath, default_state)
    """
    Persiste el estado por request_id:

        {
            "items": {   #  item_id -> item_data
                item_id: item_data,
                ...
            }
        }

    Formato en archivo (por request_id):
        item_id;item_data
        ...
    """

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde un archivo específico para un request_id.

        Formatos esperados (una por línea):
            item_id;item_data
        """
        state = self.data_by_request.setdefault(
            request_id,
            {
                "items": {},
            },
        )

        items = {}

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            if not parts:
                continue
            
            if len(parts) != 2:
                logging.warning(
                    f"action: invalid_user_line_format | request_id: {request_id} | line: {line}"
                )
                continue
            item_id_str, item_data = parts
            try:
                item_id = int(item_id_str)
            except ValueError as e:
                logging.warning(
                    f"action: invalid_item_id | request_id: {request_id} "
                    f"| line: {line} | error: {e}"
                )
                continue
            items[item_id] = item_data
            
        state["items"] = items


    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en el archivo específico para un request_id.

        Formato:
           item_id;item_data
        """
        state = self.get_state(request_id)
        
        if not state:
            logging.warning(
                f"action: save_state_no_data | request_id: {request_id}"
            )
            return

        items = state.get("items", {})
        
        # Último mensaje por sender
        chunk = ''
        for item_id, item_data in items.items():
            line = f"{item_id};{item_data}\n"
            chunk += line
            
        file_handle.write(chunk)

