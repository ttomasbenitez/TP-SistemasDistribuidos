from pkg.message.message import Message
from pkg.dedup.base import DedupStrategy
import logging
from pkg.storage.state_storage.dedup_storage import DedupStorage

MAX_PENDING_SIZE = 5000
SNAPSHOT_INTERVAL = 1000

class SlidingWindowDedupStrategy(DedupStrategy):
    
    def __init__(self, total_shards: int, storage_dir: str):
        self.total_shards = total_shards
        # último msg_num contiguo visto por request_id
        self.last_contiguous_msg_num = {}   # request_id -> int
        # mensajes vistos pero aún no contiguos
        self.pending_messages = {}          # request_id -> set[int]
        # storage persistente
        self.state_storage = DedupStorage(storage_dir)
        # cuántas veces se actualizó un request_id desde el último snapshot
        self.snapshot_interval = {}         # request_id -> int
        self.current_msg_num = {}
    
    def is_duplicate(self, message: Message) -> bool:
        request_id = message.request_id
        msg_num = message.msg_num
        
        # Inicialización por request_id
        if request_id not in self.last_contiguous_msg_num:
            self.last_contiguous_msg_num[request_id] = -1
            self.pending_messages[request_id] = set()
            self.snapshot_interval[request_id] = 0
            self.current_msg_num[request_id] = -1
            
        last_cont = self.last_contiguous_msg_num[request_id]
        
        # Primer mensaje que vemos para este request_id
        if last_cont == -1:
            # Lo aceptamos y lo marcamos como contiguo inicial
            self.last_contiguous_msg_num[request_id] = msg_num
            logging.info(
                f"action: first_message | request_id: {request_id} "
                f"| msg_num: {msg_num}"
            )
            # Persisto estado inicial
            self.save_dedup_state(message)
            return False
        
        # Si el msg_num es <= último contiguo, es duplicado (o viejo)
        if msg_num <= last_cont:
            logging.info(
                f"action: duplicate_or_old_message | request_id: {request_id} "
                f"| msg_num: {msg_num} | last_contiguous: {last_cont}"
            )
            return True
        
        # Si ya estaba en pending, es duplicado
        if msg_num in self.pending_messages[request_id]:
            logging.info(
                f"action: duplicate_pending_message | request_id: {request_id} "
                f"| msg_num: {msg_num}"
            )
            return True
         
        # Adelantado: lo agregamos a pending
        self.pending_messages[request_id].add(msg_num)
        logging.info(
            f"action: added_to_pending | request_id: {request_id} "
            f"| msg_num: {msg_num} | pending_size: {len(self.pending_messages[request_id])}"
        )
        
        # Intentamos avanzar la ventana si corresponde
        self._advance_window_if_possible(message)
        # Limpiamos la ventana si se hizo demasiado grande
        self._clean_window_if_needed(message)
    
        return False
    
    def load_dedup_state(self):
        """Restaura el estado persistido al iniciar el proceso."""
        self.state_storage.load_state_all()
       
        for request_id, state in self.state_storage.data_by_request.items():
            self.last_contiguous_msg_num[request_id] = state.get('last_contiguous_msg_num', -1)
            # me aseguro que pending sea un set
            pending = state.get('pending_messages', set())
            if not isinstance(pending, set):
                pending = set(pending)
            self.pending_messages[request_id] = pending
            self.snapshot_interval[request_id] = 0  # arranco el contador desde cero
            self.current_msg_num[request_id] = state.get('current_msg_num', -1)

            logging.info(
                f"action: dedup_state_restored | request_id: {request_id} "
                f"| last_contiguous: {self.last_contiguous_msg_num[request_id]} "
                f"| pending_count: {len(self.pending_messages[request_id])}"
                f"| current_msg_num: {self.current_msg_num[request_id]}"
            )
    
    def _advance_window_if_possible(self, message: Message):
        """
        Intenta avanzar last_contiguous_msg_num si el mensaje actual
        tiene todos sus predecesores de shard ya confirmados.
        """
        request_id = message.request_id
        msg_num = message.msg_num
        last_cont = self.last_contiguous_msg_num[request_id]
        
        # Mensaje anterior en ESTE shard
        prev_expected = msg_num - self.total_shards
        
        # Si todos los predecesores (de este shard) ya están antes o al nivel
        # del último contiguo, puedo avanzar el contiguo hasta msg_num.
        if prev_expected <= last_cont:
            self.last_contiguous_msg_num[request_id] = msg_num

            # Puede que este msg estuviera en pending o no.
            self.pending_messages.setdefault(request_id, set())
            self.pending_messages[request_id].discard(msg_num)

            # Ahora avanzo mientras haya siguientes en pending
            current_check = msg_num + self.total_shards
            while current_check in self.pending_messages[request_id]:
                self.last_contiguous_msg_num[request_id] = current_check
                self.pending_messages[request_id].remove(current_check)
                current_check += self.total_shards

            logging.info(
                f"action: window_advanced | request_id: {request_id} "
                f"| new_last_contiguous: {self.last_contiguous_msg_num[request_id]} "
                f"| pending_size: {len(self.pending_messages[request_id])}"
            )
    
    def _clean_window_if_needed(self, message: Message):
        """
        Si la ventana de pending se hace demasiado grande, la recorto.
        Mantengo solo los últimos MAX_PENDING_SIZE msg_num más grandes,
        y dejo intacto last_contiguous_msg_num.
        """
        request_id = message.request_id
        pending = self.pending_messages.get(request_id, set())
        
        if len(pending) > MAX_PENDING_SIZE:
            logging.info(
                f"action: pending_window_too_large | request_id: {request_id} "
                f"| size: {len(pending)} | max_size: {MAX_PENDING_SIZE}"
            )
            # Me quedo con los msg_num más grandes (los más recientes)
            sorted_pending = sorted(pending)
            to_keep = set(sorted_pending[-MAX_PENDING_SIZE:])
            dropped = len(pending) - len(to_keep)
            self.pending_messages[request_id] = to_keep

            logging.info(
                f"action: pending_window_shrunk | request_id: {request_id} "
                f"| kept: {len(to_keep)} | dropped: {dropped}"
            )
    
    def save_dedup_state(self, message: Message):
        """Maneja snapshot vs append para el estado de dedup."""
        request_id = message.request_id

        # Actualizo el dict que la storage usa como fuente de verdad
        self.state_storage.data_by_request[request_id] = {
            'pending_messages': self.pending_messages[request_id],
            'last_contiguous_msg_num': self.last_contiguous_msg_num[request_id],
            'current_msg_num': self.current_msg_num[request_id],
        }
        
        # Contador por request_id para decidir cuándo hacer snapshot
        self.snapshot_interval.setdefault(request_id, 0)
        self.snapshot_interval[request_id] += 1
        
        if self.snapshot_interval[request_id] >= SNAPSHOT_INTERVAL:
            logging.info(
                f"action: saving_dedup_snapshot | request_id: {request_id} "
                f"| msg_num: {message.msg_num} "
                f"| last_contiguous: {self.last_contiguous_msg_num[request_id]} "
                f"| pending_size: {len(self.pending_messages[request_id])}"
                f" | current_msg_num: {self.current_msg_num[request_id]}"
            )
            self.snapshot_interval[request_id] = 0
            self.state_storage.save_state(request_id)
        else:
            self.state_storage.append_state(request_id)
        
    def clean_dedup_state(self, message: Message):
        """Limpia el estado de dedup en memoria y en disco para un request_id."""
        request_id = message.request_id

        if request_id in self.last_contiguous_msg_num:
            del self.last_contiguous_msg_num[request_id]
        if request_id in self.pending_messages:
            del self.pending_messages[request_id]
        if request_id in self.snapshot_interval:
            del self.snapshot_interval[request_id]
        if request_id in self.current_msg_num:
            del self.current_msg_num[request_id]
        
        self.state_storage.delete_state(request_id)
        logging.info(
            f"action: dedup_state_cleared | request_id: {request_id}"
        )
