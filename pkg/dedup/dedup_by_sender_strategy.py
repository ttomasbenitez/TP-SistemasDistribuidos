from pkg.dedup.base import DedupStrategy
from pkg.message.message import Message
import logging
from pkg.storage.state_storage.dedup_by_sender_storage import DedupBySenderStorage

SNAPSHOT_INTERVAL = 1000

class DedupBySenderStrategy(DedupStrategy):
    
    def __init__(self, storege_dir: str):
        self.state_storage = DedupBySenderStorage(storege_dir)
        self.snapshot_interval = {}
    
    def is_duplicate(self, message: Message):
        """
        Returns True if the message is a duplicate or out-of-order for the given stream
        (based on last seen msg_num for its sender+request). Updates last seen on accept.
        """
        key = message.get_node_id()
        state = self.state_storage.get_state(message.request_id)
        last_by_sender = state.get("last_by_sender", {})
        last = last_by_sender.get(key, -1)
        
        if message.msg_num <= last:
            if message.msg_num == last:
                logging.info(f"action: duplicate_msg | sender:{key} | msg:{message.msg_num} | last:{last} | request_id:{message.request_id}")
            else:
                logging.warning(f"action: out_of_order_msg | sender:{key} | msg:{message.msg_num} < last:{last} | request_id:{message.request_id}")
            return True
        
        state["last_by_sender"][key] = message.msg_num
        self.state_storage.data_by_request[message.request_id] = state
        
        self.snapshot_interval.setdefault(message.request_id, 0)
        self.snapshot_interval[message.request_id] += 1
        
        if self.snapshot_interval[message.request_id] >= SNAPSHOT_INTERVAL:
            logging.info(f"action: saving dedup state snapshot | request_id: {message.request_id} | msg_num: {message.msg_num} | last_contiguous: {self.last_contiguous_msg_num[message.request_id]} | pending_size: {len(self.pending_messages[message.request_id])}")
            self.snapshot_interval[message.request_id] = 0
            self.state_storage.save_state(message.request_id)
        else:
            self.state_storage.append_state(message.request_id)
        
        return False
    
    def save_dedup_state(self, message: Message):
        self.state_storage.save_state(message.request_id)
        
    def append_dedup_state(self, request_id):
        self.state_storage.append_state(request_id)
    
    def load_dedup_state(self):
        self.state_storage.load_state_all()
        
    def clean_dedup_state(self, request_id):
        self.state_storage.delete_state(request_id)