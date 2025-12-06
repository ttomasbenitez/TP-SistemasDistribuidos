from pkg.storage.state_storage.base import StateStorage

class DedupBySenderStorage(StateStorage):
    
    def __init__(self, storage_dir):
        default_state = {
            "last_by_sender": dict(),
        }
        
        filepath = f"{storage_dir}/dedup_by_sender_storage"
        super().__init__(filepath, default_state)

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id con formato compacto:
            sender_id;last_num_sent

        Rellena:
            state["last_by_sender"]
           
        """
        
        state = self.data_by_request.setdefault(request_id, {
            "last_by_sender": None,
        })

        last_by_sender = dict()
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            if len(parts) != 2:
                raise ValueError(f"Formato inválido en línea: {line}")
            

            sender_id = parts[0]
            last_num_sent = int(parts[1])
            last_by_sender[sender_id] = max(last_num_sent, last_by_sender.get(sender_id, -1))
            
        state["last_by_sender"] = last_by_sender
            
    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en formato compacto:
            sender_id;last_num_sent
            sender_id;last_num_sent
            ...
        """
        
        last_by_sender = self.get_state(request_id)["last_by_sender"]
        chunk = ''
        
        for sender_id, last_num_sent in last_by_sender.items():
            line = f"{sender_id};{last_num_sent}\n"
            chunk += line
            
        file_handle.write(chunk)
        
        
        
