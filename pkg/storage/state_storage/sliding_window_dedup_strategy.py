from pkg.storage.state_storage.base import StateStorage
import logging

class SlidingWindowDedupStrategyStateStorage(StateStorage):
    
    def __init__(self, storage_dir):
        # Defaults reales
        default_state = {
            "current_msg_num": -1,
            "last_contiguous_msg_num": -1,
            "pending_messages": set(),
        }
        
        filepath = f"{storage_dir}/dedup_storage"
        super().__init__(filepath, default_state)

    def _safe_int(self, value, field_name, line, request_id, default=-1):
        """
        Intenta parsear un int; si falla, loguea y devuelve default.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            logging.warning(
                f"action: dedup_state_invalid_int | request_id: {request_id} "
                f"| field: {field_name} | value: {value} | line: {line}"
            )
            return default

    def _load_state_from_file(self, file_handle, request_id):
        """
        Formato:
            M;current_msg_num;last_contiguous_msg_num
            P;pend1;pend2;pend3;...

        Rellena:
            state["current_msg_num"]
            state["last_contiguous_msg_num"]
            state["pending_messages"]
        """
        state = self.data_by_request.setdefault(request_id, {
            "current_msg_num": -1,
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

            if tag == "M":
                if len(parts) != 3:
                    logging.warning(
                        f"action: dedup_state_invalid_M_format | request_id: {request_id} | line: {line}"
                    )
                    continue

                curr_prev = state.get("current_msg_num", -1)
                last_prev = state.get("last_contiguous_msg_num", -1)

                curr_val = self._safe_int(parts[1], "current_msg_num", line, request_id, default=curr_prev)
                last_val = self._safe_int(parts[2], "last_contiguous_msg_num", line, request_id, default=last_prev)

                state["current_msg_num"] = max(curr_prev, curr_val)
                state["last_contiguous_msg_num"] = max(last_prev, last_val)
            
            elif tag == "P":
                # Caso "P;" o "P;;" → sin pendientes
                if len(parts) == 1 or (len(parts) == 2 and parts[1] == ""):
                    continue

                for p in parts[1:]:
                    p = p.strip()
                    if not p:
                        continue
                    val = self._safe_int(p, "pending_msg_num", line, request_id, default=None)
                    if val is not None:
                        pending_set.add(val)

            else:
                logging.warning(
                    f"action: dedup_state_unknown_tag | request_id: {request_id} "
                    f"| tag: {tag} | line: {line}"
                )
                continue

        logging.info(
            f"action: dedup_state_loaded | request_id: {request_id} "
            f"| current_msg_num: {state['current_msg_num']} "
            f"| last_contiguous_msg_num: {state['last_contiguous_msg_num']} "
            f"| pending_count: {len(state['pending_messages'])}"
        )

    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en formato compacto:
            M;current_msg_num;last_contiguous_msg_num
            P;pend1;pend2;pend3;...
        """
        if request_id not in self.data_by_request:
            return

        state = self.get_state(request_id)
        current_msg_num = state.get("current_msg_num", -1)
        last_contiguous_msg_num = state.get("last_contiguous_msg_num", -1)
        pending = sorted(state.get("pending_messages", set()))

        # Aseguramos que nunca se escriba None
        if current_msg_num is None:
            current_msg_num = -1
        if last_contiguous_msg_num is None:
            last_contiguous_msg_num = -1

        file_handle.write(f"M;{current_msg_num};{last_contiguous_msg_num}\n")

        if pending:
            pending_str = ";".join(str(p) for p in pending)
            file_handle.write(f"P;{pending_str}\n")
