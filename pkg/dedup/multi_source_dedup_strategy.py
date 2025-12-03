import os
from pkg.dedup.sliding_window_dedup_strategy import SlidingWindowDedupStrategy
from pkg.message.message import Message
import logging

class MultiSourceDedupStrategy:
    """
    Dedup strategy for messages coming from multiple source replicas.
    Each replica sends messages in a specific range:
    - Replica 1: starts at 1000000
    - Replica 2: starts at 2000000
    - Replica N: starts at N * 1000000
    
    Messages from each replica are sequential (increment by 1).
    We maintain a separate SlidingWindowDedupStrategy for each replica.
    """
    def __init__(self, base_storage_dir: str):
        self.base_storage_dir = base_storage_dir
        self.strategies = {}

    def _get_strategy(self, replica_id: int) -> SlidingWindowDedupStrategy:
        if replica_id not in self.strategies:
            storage_dir = os.path.join(self.base_storage_dir, f"replica_{replica_id}")
            # Each replica sends sequential messages (increment by 1)
            # Starting message number is replica_id * 1000000
            start_msg_num = replica_id * 1000000
            # total_shards=1 because messages increment by 1, not by a shard count
            self.strategies[replica_id] = SlidingWindowDedupStrategy(
                total_shards=1, 
                storage_dir=storage_dir,
                start_msg_num=start_msg_num
            )
            self.strategies[replica_id].load_dedup_state()
            logging.info(f"Created dedup strategy for replica {replica_id} with start_msg_num={start_msg_num}")
        return self.strategies[replica_id]

    def _get_replica_id(self, msg_num: int) -> int:
        """Extract replica ID from message number (msg_num // 1000000)"""
        return msg_num // 1000000

    def check_state_before_processing(self, message: Message) -> bool:
        replica_id = self._get_replica_id(message.msg_num)
        strategy = self._get_strategy(replica_id)
        return strategy.check_state_before_processing(message)

    def update_contiguous_sequence(self, message: Message):
        replica_id = self._get_replica_id(message.msg_num)
        strategy = self._get_strategy(replica_id)
        strategy.update_contiguous_sequence(message)

    def update_state_on_eof(self, message: Message):
        # Clear the request state from all loaded strategies
        for strategy in self.strategies.values():
            strategy.update_state_on_eof(message)
