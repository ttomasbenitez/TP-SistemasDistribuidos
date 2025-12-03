import json
import os
import threading
from typing import Dict
from pkg.message.message import Message
from pkg.storage.format_results import FormatResults


class QueryBuf:
    def __init__(self, file_path: str, lock: threading.Lock):
        self.file_path = file_path
        self.eof = False
        self._lock = lock

    def append(self, message: Message):
        """Agrega un chunk de resultados al archivo NDJSON (una línea por item)."""
        formatter = FormatResults(message)
        data = formatter.pre_process_chunk()

        with self._lock:
            with open(self.file_path, "a", encoding="utf-8") as f:
                for data_item in data:
                    f.write(json.dumps(data_item, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())


class RunBuf:
    """Agrupa el estado de un request_id (un archivo único NDJSON)."""
    def __init__(self, run_id: str, file_path: str, lock: threading.Lock):
        self.run_id = run_id
        self.file_path = file_path
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        self.query_buf = QueryBuf(self.file_path, lock)


class ResultStorage:
    """
    Uso:
        storage = ResultStorage("storage")
        storage.start_run(request_id)
        storage.add_chunk(message)
        storage.close_run(request_id)
    """
    def __init__(self, file_path: str):
        self._runs: Dict[str, RunBuf] = {}
        self.file_path = file_path
        self._lock = threading.Lock()

    def start_run(self, request_id: str):
        """Inicializa un nuevo buffer de resultados para este request_id."""
        if request_id not in self._runs:
            self._runs[request_id] = RunBuf(request_id, self.file_path, self._lock)

    def close_run(self, request_id: str):
        """Marca el run como terminado (opcional, limpieza)."""
        self._runs.pop(request_id, None)

    def add_chunk(self, message: Message):
        """Agrega un chunk de datos al archivo del request_id."""
        rid = message.request_id
        rb = self._runs.get(rid)

        if not rb:
            rb = RunBuf(rid, self.file_path, self._lock)
            self._runs[rid] = rb

        qbuf = rb.query_buf
        if not qbuf.eof:
            qbuf.append(message)
