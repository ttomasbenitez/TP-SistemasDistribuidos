import os
import logging
from abc import ABC, abstractmethod
import threading

class StateStorage(ABC):
    
    def __init__(self, storage_dir: str):
        self.data_by_request = dict()
        self.storage_dir = storage_dir
        self._lock = threading.Lock()
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        
    def _load_state(self):
        """Carga el estado desde los archivos en el directorio de almacenamiento."""
        logging.info(f"Cargando estado desde {self.storage_dir}")
        for filename in os.listdir(self.storage_dir):
            if not filename.endswith(".txt"):
                continue
                
            try:
                request_id = int(filename.split(".")[0])
                filepath = os.path.join(self.storage_dir, filename)
                
                with self._lock:
                    with open(filepath, "r") as f:
                        self._load_state_from_file(f, request_id)
                        
                logging.info(f"Estado cargado para request_id: {request_id}")
            except Exception as e:
                logging.error(f"Error al cargar estado de {filename}: {e}")

    @abstractmethod
    def _load_state_from_file(self, file_handle, request_id):
        raise NotImplementedError("Este método debe ser implementado por subclases.")
    
    def save_state(self, request_id):
        """
        Guarda en disco el estado NUEVO de un request_id agregando
        al archivo existente, y luego limpia el buffer en RAM.
        """
        if request_id not in self.data_by_request:
            return

        final_filepath = os.path.join(self.storage_dir, f"{request_id}.txt")

        try:
            os.makedirs(self.storage_dir, exist_ok=True)

            with self._lock:
                with open(final_filepath, "a") as f:
                    self._save_state_to_file(f, request_id)
                    f.flush()
                    os.fsync(f.fileno())
                self.cleanup_state(request_id)

            logging.debug(f"Estado (append) guardado para request_id: {request_id}")
            
        except Exception as e:
            logging.error(f"Error al guardar estado para request_id {request_id}: {e.args}")
            
    @abstractmethod
    def _save_state_to_file(self, file_handle, request_id):
        raise NotImplementedError("Este método debe ser implementado por subclases.")
    
    def delete_state(self, request_id):
        """Borra el estado de un request_id de memoria y disco."""
        if request_id in self.data_by_request:
            del self.data_by_request[request_id]
            
        filepath = os.path.join(self.storage_dir, f"{request_id}.txt")
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                logging.info(f"Estado borrado para request_id: {request_id}")
            except Exception as e:
                logging.error(f"Error al borrar archivo de estado {filepath}: {e}")

    def cleanup_state(self, request_id):
        """Limpia el estado en memoria para un request_id específico."""
        if request_id in self.data_by_request:
            del self.data_by_request[request_id]
            logging.debug(f"Estado en memoria limpiado para request_id: {request_id}")