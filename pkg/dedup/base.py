from abc import ABC, abstractmethod
from pkg.message.message import Message

class DedupStrategy(ABC):
    @abstractmethod
    def is_duplicate(self, message: Message):
        pass
    
    @abstractmethod
    def check_state_before_processing(self, message: Message):
        pass
    
    @abstractmethod
    def load_dedup_state(self):
        pass