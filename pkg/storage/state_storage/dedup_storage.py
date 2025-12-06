from pkg.storage.state_storage.base import StateStorage

class DedupStorage(StateStorage):
    
    def __init__(self, storage_dir):
        default_state = {
            'last_contiguous_msg_num': int,
            'pending_messages': set,
            'current_msg_num': int
        }
        
        filepath = f"{storage_dir}/dedup_storage"
        super().__init__(filepath, default_state)


    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id con formato compacto:
        
            M;current_msg_num;last_contiguous_msg_num
            P;pend1;pend2;pend3;...

        Rellena:
            state["msg_num"]
            state["last_contiguous_msg_num"]
            state["pending_messages"]
        """
        # Crear contenedor vacío si aún no existe
        state = self.data_by_request.setdefault(request_id, {
            "msg_num": -1,
            "last_contiguous_msg_num": -1,
            "pending_messages": set(),
        })

        pending_set = state["pending_messages"]
        pending_set.clear()

        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")

            tag = parts[0]

            # -------------------------
            # Línea M
            # -------------------------
            if tag == "M":
                if len(parts) != 3:
                    raise ValueError(f"Formato inválido en línea M: {line}")

                state["msg_num"] = max(int(parts[1]), state.get("msg_num", -1))
                state["last_contiguous_msg_num"] = max(int(parts[2]), state.get("last_contiguous_msg_num", -1))
            
            # -------------------------
            # Línea P
            # -------------------------
            elif tag == "P":
                # Caso "P;" sin pendientes
                if len(parts) == 1 or (len(parts) == 2 and parts[1] == ""):
                    continue

                # parts[1:] son los pending
                for p in parts[1:]:
                    if p:  # evitar vacío
                        pending_set.add(int(p))

            else:
                raise ValueError(f"Línea desconocida en estado: {line}")

    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en formato compacto:
            M;current_msg_num;last_contiguous_msg_num
            P;pend1;pend2;pend3;...
        """
        if request_id not in self.data_by_request:
            return

        state = self.get_state(request_id)
        current_msg_num = state.get('msg_num')
        last_contiguous_msg_num = state.get('last_contiguous_msg_num')
        pending = sorted(state.get('pending_messages', set()))

        # Línea M
        file_handle.write(f"M;{current_msg_num};{last_contiguous_msg_num}\n")

        # Línea P compacta
        if pending:
            # join automático para los `P;v1;v2;v3;...`
            pending_str = ";".join(str(p) for p in pending)
            file_handle.write(f"P;{pending_str}\n")
        else:
            # Si no hay pendientes igualmente escribimos la línea P para consistencia
            file_handle.write("P;\n")
        
