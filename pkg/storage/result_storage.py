import json
import os
from typing import Dict
from pkg.message.message import Message
from pkg.storage.format_results import FormatResults

class QueryBuf:
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.eof = False

    def append(self, message: Message):
        formatter = FormatResults(message)
        data = formatter.pre_process_chunk()
        with open(self.file_path, "a", encoding="utf-8") as f:
            for dataItem in data:
                f.write(json.dumps(dataItem, ensure_ascii=False) + "\n")

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
    def __init__(self, file_path):
        self._runs: Dict[str, RunBuf] = {}
        self.file_path = file_path

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
            rb.queries[message.type] = QueryBuf(self.file_path)
        q = rb.queries[message.type]
        if not q.eof:
            q.append(message)
                