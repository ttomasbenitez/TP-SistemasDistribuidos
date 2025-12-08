from pkg.storage.state_storage.base import StateStorage
from pkg.message.q4_result import Q4IntermediateResult
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.utils import  parse_int, parse_float
from abc import ABC, abstractmethod
from pkg.message.message import Message
from pkg.message.q2_result import Q2Result


class JoinerStateStorage(StateStorage, ABC):
    
    def __init__(self, storage_dir):
        # Defaults reales
        default_state = {
            "last_by_sender": {},
            "pending_results": [],
            "eofs_count": 0,
            "current_msg_num": -1
        }
        
        filepath = f"{storage_dir}/joiner_storage"
        super().__init__(filepath, default_state)
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            SE;sender_id;last_num_sent
            PC;serialized_client_data
            EC;eofs_count

        Estructura de destino:
            self.data_by_request[request_id] = {
                "last_by_sender": { `sender_id` : last_num_sent },
                "pending_results": [],
                "eofs_count": int,
                "current_msg_num": int
            }
        """

        state = self.data_by_request.setdefault(request_id, {
            "last_by_sender": {},
            "pending_results": [],
            "eofs_count": 0,
            "current_msg_num": -1
        })

        last_by_sender = state["last_by_sender"]
        pending_results = state["pending_results"]
        eofs_count = state["eofs_count"]
        current_msg_num = state["current_msg_num"]
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")

            kind = parts[0]

            if kind == "SE":
                _k, sender_key, last_str = parts

                try:
                    last_num = int(last_str)
                except ValueError as e:
                    continue

                last_by_sender[sender_key] = max(last_by_sender.get(sender_key, -1), last_num)
                continue
            
            if kind == "PC":
                pending_results.append(self._load_pending_clients(parts))
               
            if kind == "LE":
                _k, last_eof_str = parts
                try:
                    last_eof = int(last_eof_str)
                except ValueError as e:
                    continue
                eofs_count = max(state["eofs_count"], last_eof)
                continue
            if kind == "CM":
                _k, current_msg_num_str = parts
                try:
                    msg_num = int(current_msg_num_str)
                except ValueError as e:
                    continue
                current_msg_num = max(state["current_msg_num"], msg_num)
                continue
                
        self.data_by_request[request_id]["last_by_sender"] = last_by_sender
        self.data_by_request[request_id]["pending_results"] = pending_results
        self.data_by_request[request_id]["eofs_count"] = eofs_count
        self.data_by_request[request_id]["current_msg_num"] = current_msg_num
                
            
    def _append_sender_data(self, last_by_sender, file_handle):
        for sender_id, last_num in last_by_sender.items():
            line = f"SE;{sender_id};{last_num}\n"
            file_handle.write(line)
            
    def _append_pending_results(self, pending_results, file_handle):
        for q4_intermediate_result in pending_results:
            serialialized = q4_intermediate_result.serialize()
            line = f"PC;{serialialized}"
            file_handle.write(line)
            
    def _append_last_eofs(self, last_eofs_by_node, file_handle):
        for node_id, last_eof in last_eofs_by_node.items():
            line = f"LE;{node_id};{last_eof}\n"
            file_handle.write(line)
    
    def _save_state_to_file(self, file_handle, request_id):
        state = self.data_by_request.get(request_id)
        
        if not state:
            return

        self._append_sender_data(state.get("last_by_sender", {}), file_handle)
        self._append_pending_results(state.get("pending_results", []), file_handle)
        eofs_count = state.get("eofs_count", 0)
        file_handle.write(f"LE;{eofs_count}\n")
        current_msg_num = state.get("current_msg_num", -1)
        file_handle.write(f"CM;{current_msg_num}\n")
            
    @abstractmethod
    def _load_pending_clients(self, parts):
        raise NotImplementedError("Subclasses must implement this method") 
        
        
class JoinerQ2StateStorage(JoinerStateStorage):
    
    def _load_pending_clients(self, parts):
        _k, year_month_created_at, item_data, value, result_type = parts
        return Q2Result(year_month_created_at, item_data, value, result_type)

class JoinerQ3StateStorage(JoinerStateStorage):
    
    def _load_pending_clients(self, parts):
        _k, period, store_id, tpv = parts
        try:    
            store_id = parse_int(store_id)
            tpv = parse_float(tpv)
        except ValueError as e:
            return None
        return Q3IntermediateResult(period, store_id, tpv)
     
class JoinerQ4StateStorage(JoinerStateStorage):
    
    def _load_pending_clients(self, parts):
        _k, store_id_str, user_id, purchases_qty = parts
        try:    
            store_id = parse_int(store_id_str)
        except ValueError as e:
            return None
        return Q4IntermediateResult(store_id, user_id, purchases_qty)
    
