from pkg.storage.state_storage.base import StateStorage
from pkg.message.q2_result import Q2IntermediateResult
import logging

class QuantityAndProfitStateStorage(StateStorage):
    
    def __init__(self, storage_dir):
        default_state = {
            "items_by_ym": {},  
            "last_by_sender": {},
        }
        
        filepath = f"{storage_dir}/quantity_and_profit_storage"
        super().__init__(filepath, default_state)

    def _load_state_from_file(self, file_handle, request_id):
        """
        Carga el estado desde un archivo específico para un request_id.
        
        Formatos (ÚNICOS formatos soportados):
            I;ym;item_data               # item acumulado por ym e item_id
            S;sender_id;last_msg_num     # último mensaje por sender
        """
        state = self.data_by_request.setdefault(
            request_id,
            {
                "items_by_ym": {},
                "last_by_sender": {},
            },
        )
        
        items_by_ym = state["items_by_ym"]
        last_by_sender = state["last_by_sender"]

        loaded_items = 0
        loaded_senders = 0
        
        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(";", 2)
            if len(parts) != 3:
                logging.warning(
                    f"action: invalid_line_format | request_id: {request_id} | line: {line}"
                )
                continue

            kind, a, b = parts

            try:
                if kind == "I":
                    # I;ym;item_data
                    ym, item_data = a, b
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

                elif kind == "S":
                    # S;sender_id;last_msg_num
                    sender_id, last_num_str = a, b
                    last_num = int(last_num_str)
                    last_by_sender[sender_id] = last_num
                    loaded_senders += 1
                    logging.info(
                        f"action: loaded_last_by_sender | request_id: {request_id} "
                        f"| sender_id: {sender_id} | last_msg_num: {last_num}"
                    )

                else:
                    logging.warning(
                        f"action: unknown_record_kind | request_id: {request_id} "
                        f"| kind: {kind} | line: {line}"
                    )
                    continue

            except Exception as e:
                logging.warning(
                    f"action: failed_to_load_record | request_id: {request_id} "
                    f"| line: {line} | error: {e}"
                )
                continue
            
        state["items_by_ym"] = items_by_ym
        state["last_by_sender"] = last_by_sender

        total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
        total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
        logging.info(
            f"action: load_state_summary | request_id: {request_id} "
            f"| loaded_items: {loaded_items} | loaded_senders: {loaded_senders} "
            f"| total_qty: {total_qty} | total_sub: {total_sub}"
        )
            
    def _save_state_to_file(self, file_handle, request_id):
        """
        Guarda el estado en un archivo específico para un request_id.

        Formato que ESCRIBE:
            I;ym;item_data               # por cada item acumulado
            S;sender_id;last_msg_num     # por cada sender con último mensaje
        """
        state = self.get_state(request_id)
        
        if not state:
            logging.warning(f"action: save_state_no_data | request_id: {request_id}")
            return
        
        items_by_ym = state.get("items_by_ym", {})
        last_by_sender = state.get("last_by_sender", {})

        saved_items = 0
        saved_senders = 0
        chunk = ''
        
        for ym, items_dict in items_by_ym.items():
            for item in items_dict.values():
                line = f"I;{ym};{item.serialize()}\n"
                chunk += line
                saved_items += 1
        
        for sender_id, last_num in last_by_sender.items():
            line = f"S;{sender_id};{last_num}\n"
            chunk += line
            saved_senders += 1
        
        file_handle.write(chunk)

