from pkg.message.message import Message
from pkg.dedup.base import DedupStrategy
import logging
from pkg.storage.state_storage.dedup_storage import DedupStorage

MAX_PENDING_SIZE = 5000

class SlidingWindowDedupStrategy(DedupStrategy):
    
    def __init__(self, total_shards: int, storage_dir: str):
        self.total_shards = total_shards
        self.last_contiguous_msg_num = {}
        self.pending_messages = {}
        self.current_msg_num = {}
        self.state_storage = DedupStorage(storage_dir, {
            'msg_num': -1,
            'last_contiguous_msg_num': -1, 
            'current_msg_num': -1
        })
    
    def is_duplicate(self, message: Message):
        last_cont = self.last_contiguous_msg_num[message.request_id]
        
        if message.msg_num <= last_cont:
            logging.info(f"action: Duplicate message received | request_id: {message.request_id} | msg_num: {message.msg_num} | last_contiguous: {last_cont}")
            return True
        
        if message.msg_num in self.pending_messages[message.request_id]:
             logging.info(f"action: Duplicate pending message received | request_id: {message.request_id} | msg_num: {message.msg_num}")
             return True
             
        return False
    
    def load_dedup_state(self):
        self.state_storage.load_state_all()
        for request_id, state in self.state_storage.data_by_request.items():
            self.last_contiguous_msg_num[request_id] = state.get('last_contiguous_msg_num', -1)
            self.pending_messages[request_id] = state.get('pending_messages', set())
            self.current_msg_num[request_id] = state.get('current_msg_num', 0)
            logging.info(f"Estado recuperado para request_id {request_id}: last_contiguous={self.last_contiguous_msg_num[request_id]}, pending_count={len(self.pending_messages[request_id])}, current_msg_num={self.current_msg_num[request_id]}")
    
    def check_state_before_processing(self, message):
        request_id = message.request_id
        
        if request_id not in self.last_contiguous_msg_num:
            self.last_contiguous_msg_num[request_id] = -1
            self.pending_messages[request_id] = set()
        
        if self.is_duplicate(message):
            return False
    
        self.pending_messages[message.request_id].add(message.msg_num)
        logging.info(f"action: Added message to pending | request_id: {message.request_id} | msg_num: {message.msg_num} | pending_size: {len(self.pending_messages[message.request_id])}")
        self._clean_window_if_needed(message.request_id)
        
    def _clean_window_if_needed(self, request_id):
        if len(self.pending_messages[request_id]) > MAX_PENDING_SIZE:
            logging.info(f"action: Clearing pending messages window | request_id: {request_id} | size: {len(self.pending_messages[request_id])}")
            self.pending_messages[request_id].clear()
            
    def update_contiguous_sequence(self, message: Message):
        last_cont = self.last_contiguous_msg_num[message.request_id]
        prev_expected = message.msg_num - self.total_shards
        
        if prev_expected <= last_cont:
            self.last_contiguous_msg_num[message.request_id] = message.msg_num
            self.pending_messages[message.request_id].remove(message.msg_num)
            
            current_check = message.msg_num + self.total_shards
            while current_check in self.pending_messages[message.request_id]:
                self.last_contiguous_msg_num[message.request_id] = current_check
                self.pending_messages[message.request_id].remove(current_check)
                current_check += self.total_shards
                
        self.state_storage.data_by_request[message.request_id] = {
            'msg_num': message.msg_num,
            'last_contiguous_msg_num': self.last_contiguous_msg_num[message.request_id], 
            'current_msg_num': self.current_msg_num.get(message.request_id, 0)
        }
        
        self.state_storage.save_state(message.request_id, False)
        
    def update_state_on_eof(self, message: Message):
        if message.request_id in self.last_contiguous_msg_num:
            del self.last_contiguous_msg_num[message.request_id]
        if message.request_id in self.pending_messages:
            del self.pending_messages[message.request_id]
        
        self.state_storage.delete_state(message.request_id)
        
