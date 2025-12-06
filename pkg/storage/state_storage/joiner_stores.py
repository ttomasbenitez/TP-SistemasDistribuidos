from pkg.storage.state_storage.base import StateStorage
from pkg.message.q4_result import Q4IntermediateResult
from pkg.message.q3_result import Q3IntermediateResult
from pkg.message.utils import  parse_int, parse_float
from abc import ABC, abstractmethod


class JoinerStoresStateStorage(StateStorage, ABC):
    
    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde archivo para un request_id.

        Formatos aceptados:
            ST;store_id;name
            SE;sender_id;last_num_sent
            PC;serialized_client_data  # Q4IntermediateResult

        Estructura de destino:
            self.data_by_request[request_id] = {
                "stores": { store_id: name },
                "last_by_sender": { `sender_id:steam` : last_num_sent },
                "pending_results": [Q4IntermediateResult]
            }
        """

        state = self.data_by_request.setdefault(request_id, {
            "stores": {},
            "last_by_sender": {},
            "pending_results": [],
            "last_eof_count": 0,
        })

        stores = state["stores"]
        last_by_sender = state["last_by_sender"]
        pending_results = state["pending_results"]
        last_eof_count = state["last_eof_count"]
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")

            kind = parts[0]

            if kind == "ST":
                _k, store_id_str, store_name = parts
                try:
                    store_id = int(store_id_str)
                except ValueError as e:
                    continue

                stores[store_id] = store_name
                continue

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
                last_eof_count = max(state["last_eof_count"], last_eof)
                continue
                
        self.data_by_request[request_id]["stores"] = stores
        self.data_by_request[request_id]["last_by_sender"] = last_by_sender
        self.data_by_request[request_id]["pending_results"] = pending_results
        self.data_by_request[request_id]["last_eof_count"] = last_eof_count
                
            
    def _append_sender_data(self, last_by_sender, file_handle):
        for sender_id, last_num in last_by_sender.items():
            line = f"SE;{sender_id};{last_num}\n"
            file_handle.write(line)
                
    def _append_stores(self, stores, file_handle):
        for store_id, name in stores.items():
            line = f"ST;{store_id};{name}\n"
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
        self._append_stores(state.get("stores", {}), file_handle)
        self._append_pending_results(state.get("pending_results", []), file_handle)
        # Persist a single last_eof_count line (format: LE;{count})
        last_eof_count = state.get("last_eof_count", 0)
        file_handle.write(f"LE;{last_eof_count}\n")
            
    @abstractmethod
    def _load_pending_clients(self, parts):
        raise NotImplementedError("Subclasses must implement this method") 
        
    
class JoinerStoresQ4StateStorage(JoinerStoresStateStorage):
    
    def _load_pending_clients(self,parts):
        _k, store_id_str, birthdate, purchases_qty = parts
        try:    
            store_id = int(store_id_str)
        except ValueError as e:
            return None
        return Q4IntermediateResult(store_id, birthdate, purchases_qty)

class JoinerStoresQ3StateStorage(JoinerStoresStateStorage):
    
    def _load_pending_clients(self ,parts):
        _k, year_half_created_at, store_id_str, intermediate_tpv_str = parts
        try:    
            store_id = parse_int(store_id_str)
            intermediate_tpv = parse_float(intermediate_tpv_str)
        except ValueError as e:
            return None
        return Q3IntermediateResult(year_half_created_at, store_id, intermediate_tpv)
            
        
        