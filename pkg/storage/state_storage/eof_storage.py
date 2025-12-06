from pkg.storage.state_storage.base import StateStorage

class EofStorage(StateStorage):
    
    def __init__(self, storage_dir):
        default_state = {
            "eofs_count": 0,
        }
        
        filepath = f"{storage_dir}/eofs_count_storage"
        super().__init__(filepath, default_state)

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id con formato compacto:
            num

        Rellena:
            state["eofs_count"]
           
        """
        
        state = self.data_by_request.setdefault(request_id, {
            "eofs_count": None,
        })

        eofs_count = 0
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            eofs_count = int(line)
            
        state["eofs_count"] = eofs_count
            
    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en formato compacto:
            sender_id;last_num_sent
        """
        
        eofs_count = self.get_state(request_id)
        line = f"{eofs_count}\n"
        file_handle.write(line)
        
        
        
