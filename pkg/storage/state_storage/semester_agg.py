from pkg.storage.state_storage.base import StateStorage
import logging


class SemesterAggregatorStateStorage(StateStorage):
    """
    Persist incremental aggregation by period/store and last-seen message numbers per upstream sender.
    File format (append-only, one record per line):
      - A;{period};{store_id};{amount}  # Aggregation delta for recovery
      - S;{sender_id};{last_msg_num}    # Last message number for dedup
    """

    def _load_state_from_file(self, file_handle, request_id, specific_state=None):
        """
        Load persisted state from file.
        Formats:
          A;period;store_id;amount     # aggregation
          S;sender_id;last_msg_num     # sender last-seen
        """
        agg_by_period = {}
        last_by_sender = {}
        
        loaded_items = 0
        loaded_senders = 0
        
        has_specific_state = specific_state is not None

        for line in file_handle:
            line = line.strip()
            if not line:
                continue
            
            try:
                parts = line.split(";")
                if len(parts) < 2:
                    continue
                    
                kind = parts[0]
                
                if kind == "A" and len(parts) == 4 and (not has_specific_state or specific_state == "agg_by_period"):
                    # Aggregation: A;period;store_id;amount
                    _, period, store_id_str, amount_str = parts
                    try:
                        store_id = int(store_id_str)
                        amount = float(amount_str)
                        
                        period_bucket = agg_by_period.setdefault(period, {})
                        period_bucket[store_id] = period_bucket.get(store_id, 0.0) + amount
                        loaded_items += 1
                        logging.debug(f"action: loaded_agg | request_id: {request_id} | period: {period} | store_id: {store_id} | amount: {amount}")
                    except (ValueError, IndexError) as e:
                        logging.warning(f"action: load_agg_error | request_id: {request_id} | line: {line} | error: {e}")
                        continue
                
                elif kind == "S" and len(parts) == 3 and (not has_specific_state or specific_state == "last_by_sender"):
                    # Sender: S;sender_id;last_msg_num
                    _, sender_id, last_str = parts
                    try:
                        last = int(last_str)
                        last_by_sender[sender_id] = max(last_by_sender.get(sender_id, -1), last)
                        loaded_senders += 1
                        logging.debug(f"action: loaded_sender | request_id: {request_id} | sender: {sender_id} | last_msg: {last}")
                    except (ValueError, IndexError) as e:
                        logging.warning(f"action: load_sender_error | request_id: {request_id} | line: {line} | error: {e}")
                        continue
                        
            except Exception as e:
                logging.warning(f"action: state_parse_error | request_id: {request_id} | line: {line} | error: {e}")
                continue
        
        # Store in data_by_request
        self.data_by_request.setdefault(request_id, {})
        self.data_by_request[request_id]["agg_by_period"] = agg_by_period
        self.data_by_request[request_id]["last_by_sender"] = last_by_sender
        
        logging.info(f"action: state_loaded | request_id: {request_id} | items: {loaded_items} | senders: {loaded_senders}")
        
    def _load_specific_state_from_file(self, file_handle, request_id, specific_state):
        """
        For compatibility; delegates to _load_state_from_file.
        """
        self._load_state_from_file(file_handle, request_id, specific_state)

    def _save_state_to_file(self, file_handle, request_id):
        """
        Writes to file using the following format per line:
          A;period;store_id;amount
          S;sender_id;last_msg_num
        
        The caller sets state["agg_by_period"] and state["last_by_sender"]
        during accumulation and we write them incrementally to disk.
        """
        state = self.data_by_request.get(request_id, {})
        
        # Write aggregations
        agg_by_period = state.get("agg_by_period", {})
        for period, store_map in agg_by_period.items():
            for store_id, amount in store_map.items():
                line = f"A;{period};{store_id};{amount}"
                file_handle.write(line + "\n")
        
        # Write sender last-msg markers
        last_by_sender = state.get("last_by_sender", {})
        for sender_id, last_msg in last_by_sender.items():
            line = f"S;{sender_id};{last_msg}"
            file_handle.write(line + "\n")
        
        logging.debug(f"action: state_written | request_id: {request_id} | periods: {len(agg_by_period)} | senders: {len(last_by_sender)}")


