from pkg.storage.state_storage.base import StateStorage

class FilterAmountStateStorage(StateStorage):

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde un archivo específico para un request_id.
        Reconstruye last_contiguous_msg_num y pending_messages.
        """
        last_contiguous = 0
        pending = set()

        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            
            try:
                parts = line.split(",")
                if len(parts) != 2:
                    continue
                    
                msg_num = int(parts[0])
                current_last_contiguous = int(parts[1])
                
                # Actualizamos el last_contiguous global si el del log es mayor
                if current_last_contiguous > last_contiguous:
                    last_contiguous = current_last_contiguous
                
                # Agregamos el mensaje actual a pendientes si es mayor que el último contiguo conocido
                if msg_num > last_contiguous:
                    pending.add(msg_num)
                    
            except ValueError:
                continue
        
        # Limpieza final: eliminar de pending cualquier mensaje que haya quedado <= last_contiguous
        # Esto puede pasar si procesamos un mensaje fuera de orden y luego llega el que faltaba
        pending = {m for m in pending if m > last_contiguous}
        
        if request_id not in self.data_by_request:
            self.data_by_request[request_id] = {}
            
        self.data_by_request[request_id]['last_contiguous_msg_num'] = last_contiguous
        self.data_by_request[request_id]['pending_messages'] = pending

    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en un archivo específico para un request_id.
        Formato: msg_num,last_contiguous_msg_num
        """
        if request_id not in self.data_by_request:
            return

        state = self.data_by_request[request_id]
        msg_num = state.get('msg_num')
        last_contiguous = state.get('last_contiguous_msg_num')
        
        if msg_num is not None and last_contiguous is not None:
            line = f"{msg_num},{last_contiguous}\n"
            file_handle.write(line)
