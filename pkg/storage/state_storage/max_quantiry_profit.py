from pkg.storage.state_storage.base import StateStorage
from pkg.message.q2_result import Q2IntermediateResult
import logging

class QuantityAndProfitStateStorage(StateStorage):

    def _load_state_from_file(self, file_handle, request_id, specific_state=None):
        """
        Carga el estado desde un archivo específico para un request_id.
        
        Formatos aceptados:
            I;ym;item_data                     # item acumulado por ym e item_id
            S;sender_id;last_msg_num           # último msg por sender (para dedup)
            ym;item_data                       # formato antiguo (legacy)
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
        eofs_by_request = state["eofs_by_request"]
        loaded_items = 0
        loaded_senders = 0        
        has_specific_state = specific_state is not None
        
        for line in file_handle:
            logging.info(f"action: processing_line | request_id: {request_id} | line: {line.strip()}")
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(";", 1)
            kind = parts[0]
            
            if kind == "I" and (not has_specific_state or specific_state == "items_by_ym"):
                # Item format: I;ym;item_data
                try:
                    rest = parts[1].split(";", 1)
                    if len(rest) != 2:
                        continue
                    ym, item_data = rest
                except (IndexError, ValueError):
                    continue
                
                try:
                    item = Q2IntermediateResult.deserialize(item_data)
                    
                    if ym not in items_by_ym:
                        items_by_ym[ym] = {}
                    
                    existing_item = items_by_ym[ym].get(item.item_id)
                    if existing_item:
                        existing_item.quantity += item.quantity
                        existing_item.subtotal += item.subtotal
                        logging.info(f"action: loaded_item_summed | request_id: {request_id} | ym: {ym} | item_id: {item.item_id} | qty: {item.quantity} | sub: {item.subtotal}")
                    else:
                        items_by_ym[ym][item.item_id] = item
                        logging.info(f"action: loaded_item_added | request_id: {request_id} | ym: {ym} | item_id: {item.item_id} | qty: {item.quantity} | sub: {item.subtotal}")
                    loaded_items += 1
                except Exception as e:
                    logging.warning(f"action: failed_to_load_item | request_id: {request_id} | error: {e}")
                    continue
                continue
            
            if kind == "S" and (not has_specific_state or specific_state == "last_by_sender"):
                # Sender format: S;sender_id;last_msg_num
                try:
                    rest_parts = parts[1].split(";")
                    if len(rest_parts) != 2:
                        continue
                    sender_id, last_str = rest_parts
                    last = int(last_str)
                    last_by_sender[sender_id] = max(last_by_sender.get(sender_id, -1), last)
                    logging.info(f"action: loaded_sender_dedup | request_id: {request_id} | sender: {sender_id} | last_msg: {last}")
                    loaded_senders += 1
                except (IndexError, ValueError) as e:
                    logging.warning(f"action: failed_to_load_sender | request_id: {request_id} | error: {e}")
                    continue
                continue

            if kind == "E" and (not has_specific_state or specific_state == "eofs_by_request"):
                # EOF format: E;eofs_by_request
                try:
                    eofs_by_request = int(parts[1])
                    logging.info(f"action: loaded_eof | request_id: {request_id} | eofs: {eofs_by_request}")
                except (IndexError, ValueError) as e:
                    logging.warning(f"action: failed_to_load_eof | request_id: {request_id} | error: {e}")
                    continue
            # Legacy format: ym;item_data (without prefix)
            try:
                if len(parts) == 2:
                    ym, item_data = parts
                    item = Q2IntermediateResult.deserialize(item_data)
                    
                    if ym not in items_by_ym:
                        items_by_ym[ym] = {}
                    
                    existing_item = items_by_ym[ym].get(item.item_id)
                    if existing_item:
                        existing_item.quantity += item.quantity
                        existing_item.subtotal += item.subtotal
                        logging.info(f"action: loaded_legacy_item_summed | request_id: {request_id} | ym: {ym} | item_id: {item.item_id}")
                    else:
                        items_by_ym[ym][item.item_id] = item
                        logging.info(f"action: loaded_legacy_item_added | request_id: {request_id} | ym: {ym} | item_id: {item.item_id}")
                    loaded_items += 1
            except Exception as e:
                logging.warning(f"action: failed_to_load_legacy_item | request_id: {request_id} | error: {e}")
                continue
        
        # Log summary
        total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
        total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
        logging.info(f"action: load_state_summary | request_id: {request_id} | loaded_items: {loaded_items} | loaded_senders: {loaded_senders} | total_qty: {total_qty} | total_sub: {total_sub}")
        
    def _load_specific_state_from_file(self, file_handle, request_id, specific_state):
        """
        For compatibility; delegates to _load_state_from_file.
        """
        self._load_state_from_file(file_handle, request_id, specific_state)
        
    def _save_state_to_file(self, file_handle, request_id):
        """Guarda el estado en un archivo específico para un request_id."""
        state = self.data_by_request.get(request_id)
        if not state:
            logging.warning(f"action: save_state_no_data | request_id: {request_id}")
            return
        
        items_by_ym = state.get("items_by_ym", {})
        last_by_sender = state.get("last_by_sender", {})
        eofs_by_request = state.get("eofs_by_request", 0)

        saved_items = 0
        saved_senders = 0
        
        # Save items
        for ym, items_dict in items_by_ym.items():
            for item in items_dict.values():
                line = f"I;{ym};{item.serialize()}\n"
                file_handle.write(line)
                saved_items += 1
                logging.info(f"action: saved_item | request_id: {request_id} | ym: {ym} | item_id: {item.item_id} | qty: {item.quantity} | sub: {item.subtotal}")
        
        # Save dedup markers
        for sender_id, last_msg_num in last_by_sender.items():
            file_handle.write(f"S;{sender_id};{last_msg_num}\n")
            saved_senders += 1
            logging.info(f"action: saved_sender_dedup | request_id: {request_id} | sender: {sender_id} | last_msg: {last_msg_num}")

        # Save EOFs
        file_handle.write(f"E;{eofs_by_request}\n")
        logging.info(f"action: saved_eof | request_id: {request_id} | eofs: {eofs_by_request}")
        
        # Log summary
        total_qty = sum(item.quantity for ym_dict in items_by_ym.values() for item in ym_dict.values())
        total_sub = sum(item.subtotal for ym_dict in items_by_ym.values() for item in ym_dict.values())
        logging.info(f"action: save_state_summary | request_id: {request_id} | saved_items: {saved_items} | saved_senders: {saved_senders} | total_qty: {total_qty} | total_sub: {total_sub}")