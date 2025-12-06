from pkg.storage.state_storage.base import StateStorage
import logging
class MessageCountStorage(StateStorage):
    
    def __init__(self, storage_dir):
        default_state = {
            'current_msg_num': -1, 
        }
        
        filepath = f"{storage_dir}/message_count_storage"
        super().__init__(filepath, default_state)
        
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id con formato compacto:
        
            current_msg_num;current_msg_num;...

        Rellena:
            state["current_msg_num"]
        """
        state = self.data_by_request.setdefault(request_id, {
            "current_msg_num": -1,
        })

        current_msg_num = state["current_msg_num"]

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = [p.strip() for p in line.split(";") if p.strip() != ""]
            if not parts:
                continue

            line_max = max(int(p) for p in parts)
            current_msg_num = max(current_msg_num, line_max)

        state["current_msg_num"] = current_msg_num
        logging.info(
            f"action: loaded_message_count | request_id: {request_id} "
            f"| current_msg_num: {current_msg_num}"
        )
        

    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en formato compacto:
            current_msg_num;
        (por snapshot) o va haciendo append de números.
        """
        if request_id not in self.data_by_request:
            return

        state = self.get_state(request_id)
        current_msg_num = state.get('current_msg_num')

        if current_msg_num is None:
            return

        line = f"{current_msg_num};"
        file_handle.write(line)
