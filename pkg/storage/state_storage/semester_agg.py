from pkg.storage.state_storage.base import StateStorage
import logging


class SemesterAggregatorStateStorage(StateStorage):
    """
    Persist incremental aggregation and last-seen message numbers per upstream sender.
    File format (append-only, one record per line):
      - agg;{period};{store_id};{delta_tpv}
      - sender;{sender_id};{last_msg_num}
    """

    def _load_state_from_file(self, file_handle, request_id):
        agg = {}
        last_by_sender = {}

        for raw in file_handle:
            line = raw.strip()
            if not line:
                continue
            try:
                parts = line.split(";")
                if parts[0] == "agg" and len(parts) == 4:
                    _, period, store_id_str, delta_str = parts
                    store_id = int(store_id_str)
                    delta = float(delta_str)
                    period_bucket = agg.setdefault(period, {})
                    period_bucket[store_id] = period_bucket.get(store_id, 0.0) + delta
                elif parts[0] == "sender" and len(parts) == 3:
                    _, sender_id, last_str = parts
                    last_by_sender[sender_id] = max(last_by_sender.get(sender_id, -1), int(last_str))
            except Exception as e:
                logging.warning(f"semester_agg state parse error: {e} on line: {line}")

        self.data_by_request.setdefault(request_id, {})
        self.data_by_request[request_id]["agg"] = agg
        self.data_by_request[request_id]["last_msg_by_sender"] = last_by_sender

    def _save_state_to_file(self, file_handle, request_id):
        """
        Writes prepared 'lines' buffer and clears it.
        Expect caller to set:
          self.data_by_request[request_id]['lines'] = [ 'agg;...;...;...', 'sender;...;...' ]
        """
        state = self.data_by_request.get(request_id, {})
        lines = state.get("lines", [])
        for line in lines:
            file_handle.write(line + "\n")

