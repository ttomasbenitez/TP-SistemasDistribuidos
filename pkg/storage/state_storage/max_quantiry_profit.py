from pkg.storage.state_storage.base import StateStorage
from pkg.message.q2_result import Q2IntermediateResult
import logging

class QuantityAndProfitStateStorage(StateStorage):

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde un archivo específico para un request_id.
        
        Formato:
            ym;item_data   # item acumulado por ym e item_id
        """
        state = self.data_by_request.setdefault(
            request_id,
            {
                "items_by_ym": {},
            },
        )
        
        items_by_ym = state["items_by_ym"]
        loaded_items = 0
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(";", 1)
            if len(parts) != 2:
                logging.warning(
                    f"action: invalid_line_format | request_id: {request_id} | line: {line}"
                )
                continue

            ym, item_data = parts
            
            try:
                item = Q2IntermediateResult.deserialize(item_data)
                
                ym_items = items_by_ym.setdefault(ym, {})
                existing_item = ym_items.get(item.item_id)

                if existing_item:
                    existing_item.quantity += item.quantity
                    existing_item.subtotal += item.subtotal
                    logging.info(
                        f"action: loaded_item_summed | request_id: {request_id} "
                        f"| ym: {ym} | item_id: {item.item_id} "
                        f"| qty: {existing_item.quantity} | sub: {existing_item.subtotal}"
                    )
                else:
                    ym_items[item.item_id] = item
                    logging.info(
                        f"action: loaded_item_added | request_id: {request_id} "
                        f"| ym: {ym} | item_id: {item.item_id} "
                        f"| qty: {item.quantity} | sub: {item.subtotal}"
                    )
                loaded_items += 1
            except Exception as e:
                logging.warning(
                    f"action: failed_to_load_item | request_id: {request_id} | error: {e}"
                )
                continue
            
        state["items_by_ym"] = items_by_ym

        total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
        total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
        logging.info(
            f"action: load_state_summary | request_id: {request_id} "
            f"| loaded_items: {loaded_items} | total_qty: {total_qty} | total_sub: {total_sub}"
        )
            
    def _save_state_to_file(self, file_handle, request_id):
        """Guarda el estado en un archivo específico para un request_id."""
        state = self.get_state(request_id)
        
        if not state:
            logging.warning(f"action: save_state_no_data | request_id: {request_id}")
            return
        
        items_by_ym = state.get("items_by_ym", {})
        
        saved_items = 0
        chunk = ''
        for ym, items_dict in items_by_ym.items():
            for item in items_dict.values():
                line = f"{ym};{item.serialize()}\n"
                chunk += line
                saved_items += 1
        
        file_handle.write(chunk)

        total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
        total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
        logging.info(
            f"action: save_state_summary | request_id: {request_id} "
            f"| saved_items: {saved_items} | total_qty: {total_qty} | total_sub: {total_sub}"
        )
