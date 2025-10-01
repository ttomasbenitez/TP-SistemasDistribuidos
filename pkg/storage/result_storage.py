import json
import os
from typing import Dict
from pkg.message.message import Message
from pkg.storage.format_results import FormatResults

class QueryBuf:
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.eof = False

        d = os.path.dirname(self.file_path)
        if d:
            os.makedirs(d, exist_ok=True)

        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump({}, f)

    def append(self, message: Message):
        
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        formatter = FormatResults(message)
        new_data = formatter.format_chunk()
        key = str(message.query_num)
        if key not in data:
            data[key] = {}
            
        data.setdefault(key, {}).update(new_data)

        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

class RunBuf:
    
    def __init__(self, client_id):
        self.client_id = client_id
        self.queries = {}
    
class ResultStorage:
    """
    Uso simple:
      storage.start_run(client_id, run_id)
      storage.add_chunk(run_id, "Q1", chunk_bytes)
      storage.mark_eof(run_id, "Q1")
    """
    def __init__(self):
        self._runs: Dict[str, RunBuf] = {}

    def start_run(self, client_id: str):
       if client_id not in self._runs:
            self._runs[client_id] = RunBuf(client_id)

    def close_run(self, run_id: str):
        self._runs.pop(run_id, None)

    def add_chunk(self, message: Message):
        rb = self._runs.get(message.request_id)
        if not rb:
            return
        if message.type not in rb.queries:
            rb.queries[message.type] = QueryBuf(f"storage/client-{message.request_id}.json")
        q = rb.queries[message.type]
        if not q.eof:
            q.append(message)
                