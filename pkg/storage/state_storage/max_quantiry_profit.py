from pkg.storage.state_storage.base import StateStorage
from pkg.message.q2_result import Q2IntermediateResult

class QuantityAndProfitStateStorage(StateStorage):

    def _load_state_from_file(self, file_handle, request_id):
        """Carga el estado desde un archivo específico para un request_id."""
        for line in file_handle:
            line = line.strip()
            if not line:
                continue
                                   
            parts = line.split(";", 1)
            ym = parts[0]
            item_data = parts[1]
            
            item = Q2IntermediateResult.deserialize(item_data)
            
            if request_id not in self.data_by_request:
                self.data_by_request[request_id] = {}
            
            if ym not in self.data_by_request[request_id]:
                self.data_by_request[request_id][ym] = {}
                
            existing_item = self.data_by_request[request_id][ym].get(item.item_id)
            if existing_item:
                existing_item.quantity += item.quantity
                existing_item.subtotal += item.subtotal
            else:
                self.data_by_request[request_id][ym][item.item_id] = item
            
    def _save_state_to_file(self, file_handle, request_id):
        """Guarda el estado en un archivo específico para un request_id."""
        for ym, items_dict in self.data_by_request[request_id].items():
            for item in items_dict.values():
                line = f"{ym};{item.serialize()}\n"
                file_handle.write(line)