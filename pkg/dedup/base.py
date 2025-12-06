from abc import ABC, abstractmethod
from pkg.message.message import Message

class DedupStrategy(ABC):
    
    @abstractmethod
    def is_duplicate(self, message: Message):
        pass
    
    @abstractmethod
    def load_dedup_state(self):
        pass
    
    @abstractmethod
    def clean_dedup_state(self, request_id):
        pass
    
    @abstractmethod
    def save_dedup_state(self, message: Message):
        pass
    
    def append_dedup_state(self, request_id):
        pass