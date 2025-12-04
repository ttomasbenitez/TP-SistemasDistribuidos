import os
import logging
from abc import ABC, abstractmethod
import threading
import copy

class StateStorage(ABC):
    
    def __init__(self, storage_dir: str, default_state: dict):
        self.data_by_request = dict()
        self.storage_dir = storage_dir
        self._lock = threading.Lock()
        self.default_state = default_state
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            
    def get_data_from_request(self, request_id):            
        """Obtiene (y si no existe, inicializa) el estado en memoria para un request_id."""
        with self._lock:
            if request_id not in self.data_by_request:
                # Copia PROFUNDA: así cada request tiene su propio dict y subdicts
                self.data_by_request[request_id] = copy.deepcopy(self.default_state)
            return self.data_by_request[request_id]
        
    def load_state(self, request_id):
        """Carga el estado desde los archivos en el directorio de almacenamiento."""
        logging.info(f"Cargando estado desde {self.storage_dir}")
        
        filepath = os.path.join(self.storage_dir, f"{request_id}.txt")
        
        if not os.path.exists(filepath):
            logging.info(f"No hay estado persistido para request_id {request_id} en {filepath}")
            return
        logging.info(f"Cargando estado desde {filepath} para request_id: {request_id}")  
        try:
            with self._lock:
                with open(filepath, "r") as f:
                    self._load_state_from_file(f, request_id)
                    
            logging.info(f"Estado cargado para request_id: {request_id}")
        except Exception as e:
            logging.error(f"Error al cargar estado de {filepath}: {e}")
            
    def load_state_all(self):
        """Carga el estado de todos los archivos en el directorio de almacenamiento."""
        logging.info(f"Cargando todos los estados desde {self.storage_dir}")
        
        try:
            for filename in os.listdir(self.storage_dir):
                if filename.endswith(".txt"):
                    request_id_str = filename[:-4]  # Remover la extensión .txt
                    try:
                        request_id = int(request_id_str)
                    except ValueError:
                        request_id = request_id_str
                    
                    filepath = os.path.join(self.storage_dir, filename)
                    
                    with self._lock:
                        with open(filepath, "r") as f:
                            self._load_state_from_file(f, request_id)
                        
                        logging.info(f"Estado cargado para request_id: {request_id}")
                    
        except Exception as e:
            logging.error(f"Error al cargar estados desde {self.storage_dir}: {e}")
            
    
    def load_specific_state_all(self, specific_state):
        """Carga el estado de todos los archivos en el directorio de almacenamiento."""
        logging.info(f"Cargando todos los estados desde {self.storage_dir}")
        
        try:
            for filename in os.listdir(self.storage_dir):
                if filename.endswith(".txt"):
                    request_id_str = filename[:-4]  # Remover la extensión .txt
                    try:
                        request_id = int(request_id_str)
                    except ValueError:
                        request_id = request_id_str
                    
                    filepath = os.path.join(self.storage_dir, filename)
                    
                    with self._lock:
                        with open(filepath, "r") as f:
                            self._load_specific_state_from_file(f, request_id, specific_state)
                        
                        logging.info(f"Estado cargado para request_id: {request_id}")
                    
        except Exception as e:
            logging.error(f"Error al cargar estados desde {self.storage_dir}: {e}")

    @abstractmethod
    def _load_state_from_file(self, file_handle, request_id):
        raise NotImplementedError("Este método debe ser implementado por subclases.")
    
    def _load_specific_state_from_file(self, file_handle, request_id, specific_state):
        pass
    
    def save_state(self, request_id, reset_state=True):
        """
        Guarda en disco el estado NUEVO de un request_id agregando
        al archivo existente, y luego limpia el buffer en RAM.
        """
        if request_id not in self.data_by_request:
            logging.error(f"No hay estado en memoria para request_id {request_id}, nada que guardar.")
            return

        final_filepath = os.path.join(self.storage_dir, f"{request_id}.txt")

        try:
            os.makedirs(self.storage_dir, exist_ok=True)

            with self._lock:
                with open(final_filepath, "a") as f:
                    self._save_state_to_file(f, request_id)
                    f.flush()
                    os.fsync(f.fileno())
                if reset_state:
                    self.cleanup_state(request_id)

            logging.debug(f"Estado (append) guardado para request_id: {request_id}")
            
        except Exception as e:
            logging.error(f"Error al guardar estado para request_id {request_id}: {e.args}")
    def save_specific_state(self, request_id, key, reset_state=False):
        """
        Guarda en disco el estado NUEVO de un request_id agregando
        al archivo existente, y luego limpia el buffer en RAM.
        """
        if request_id not in self.data_by_request:
            logging.error(f"No hay estado en memoria para request_id {request_id}, nada que guardar.")
            return

        final_filepath = os.path.join(self.storage_dir, f"{request_id}.txt")

        try:
            os.makedirs(self.storage_dir, exist_ok=True)

            with self._lock:
                with open(final_filepath, "a") as f:
                    self._save_specific_state_to_file(f, key, request_id)
                    f.flush()
                    os.fsync(f.fileno())
                if reset_state:
                    self.cleanup_state(request_id)

            logging.debug(f"Estado (append) guardado para request_id: {request_id}")
            
        except Exception as e:
            logging.error(f"Error al guardar estado para request_id {request_id}: {e.args}")
            
    
    @abstractmethod
    def _save_state_to_file(self, file_handle, request_id):
        raise NotImplementedError("Este método debe ser implementado por subclases.")
    
    def _save_specific_state_to_file(self, file_handle, key, request_id):
        pass
    
    def delete_state(self, request_id):
        """Borra el estado de un request_id de memoria y disco."""
        with self._lock:
            if request_id in self.data_by_request:
                del self.data_by_request[request_id]
            
        filepath = os.path.join(self.storage_dir, f"{request_id}.txt")
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                logging.info(f"Estado borrado para request_id: {request_id}")
            except Exception as e:
                logging.error(f"Error al borrar archivo de estado {filepath}: {e}")
                
        self.cleanup_state(request_id)

    def cleanup_state(self, request_id):
        """Limpia el estado en memoria para un request_id específico."""
        if request_id in self.data_by_request:
            del self.data_by_request[request_id]
            logging.debug(f"Estado en memoria limpiado para request_id: {request_id}")
            
    def cleanup_data(self, request_id, keys_to_maintain: list = None):
        """Limpia las claves del estado en memoria para un request_id, 
        excepto las incluidas en keys_to_maintain.
        """
        if keys_to_maintain is None:
            keys_to_maintain = ['last_by_sender']

        if request_id not in self.data_by_request:
            return
        state = self.data_by_request[request_id]
        
        # Iteramos sobre una lista de keys para poder borrar del dict
        for key in list(state.keys()):
            logging.info(f"Revisando key: {key}")
            if key not in keys_to_maintain:
                state[key].clear()
                
