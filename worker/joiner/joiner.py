
from worker.base import Worker 
from abc import ABC, abstractmethod
from utils.heartbeat import start_heartbeat_sender
import logging
from pkg.message.message import Message
from Middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddleware
from pkg.message.constants import MESSAGE_TYPE_EOF
from Middleware.connection import PikaConnection
from pkg.dedup.dedup_by_sender_strategy import DedupBySenderStrategy
from pkg.storage.state_storage.base import StateStorage
from pkg.storage.state_storage.joiner_items import JoinerItemsStateStorage

SNAPSHOT_INTERVAL = 1000

class Joiner(Worker, ABC):
    
    def __init__(self, 
                 data_input_queue: str, 
                 items_input_queue: str,
                 data_output_middleware: str,
                 storage_dir: str,
                 state_storage: StateStorage,
                 container_name :str,
                 query_num: int,
                 host: str,
                 expected_eofs):
 
        self.__init_middlewares_handler__()
        self.data_input_queue = data_input_queue
        self.data_output_middleware = data_output_middleware
        self.items_input_queue = items_input_queue
        self.node_id = container_name
        self.connection = PikaConnection(host)
        self.state_storage = state_storage
        self.joiner_storage = JoinerItemsStateStorage(storage_dir)
        self.dedup_strategy = DedupBySenderStrategy(self.state_storage)
        self.expected_eofs = expected_eofs
        self.query_num = query_num
        self.snapshot_interval = {}
        
    def start(self):
        self.heartbeat_sender = start_heartbeat_sender()
        self.state_storage.load_state_all()
        self.joiner_storage.load_state_all()
        self.connection.start()
        data_output_middleware = MessageMiddlewareExchange(self.data_output_middleware, {}, self.connection)
        self.consume_items_to_join_queue(data_output_middleware)
        self.consume_data_queue(data_output_middleware)
        
        self.connection.start_consuming()

    def consume_data_queue(self, data_output_middleware: MessageMiddleware):
        data_input_queue = MessageMiddlewareQueue(self.data_input_queue, self.connection)

        def __on_message__(msg):
            message = Message.deserialize(msg)
            
            logging.info(f"action: message received from data_queue | request_id: {message.request_id} | type: {message.type}")
            
            if message.type == MESSAGE_TYPE_EOF:
                if self.on_eof_message(message, self.dedup_strategy, self.state_storage, self.expected_eofs):
                    logging.info(f"action: EOF message processed | request_id: {message.request_id} | type: {message.type}")
                    self._process_pending_clients(message, data_output_middleware)
                    self._send_eof(message, data_output_middleware)
                return
            
            if self.dedup_strategy.is_duplicate(message):
                return
            
            try:
                self._process_items_to_join(message, data_output_middleware)
            finally:
                self.state_storage.save_state(message.request_id)
                        
        data_input_queue.start_consuming(__on_message__)

    def _send_eof(self, message: Message, data_output_middleware: MessageMiddleware):
        new_msg_num = self._get_new_msg_num(message.request_id)
        new_eof = Message(message.request_id, MESSAGE_TYPE_EOF, new_msg_num, self.query_num, self.node_id)
        self._send_message(data_output_middleware, new_eof)
        self.state_storage.delete_state(message.request_id)
        logging.info(f"action: EOF sent | request_id: {message.request_id} | type: {message.type}")

    def _get_new_msg_num(self, request_id: int) -> int:
        current_state = self.state_storage.get_state(request_id)
        current_msg_num = current_state.get("current_msg_num", -1)
        new_msg_num = current_msg_num + 1
        self.state_storage.data_by_request[request_id]["current_msg_num"] = new_msg_num
        return new_msg_num
    
    def consume_items_to_join_queue(self, data_output_middleware: MessageMiddleware):
        items_input_queue = MessageMiddlewareQueue(self.items_input_queue, self.connection)
        
        def __on_items_message__(message):
            message = Message.deserialize(message)
            logging.info(f"action: message received in items to join queue | request_id: {message.request_id} | msg_type: {message.type}")
             
            if message.type == MESSAGE_TYPE_EOF:
                if self.on_eof_message(message, self.dedup_strategy, self.state_storage, self.expected_eofs):
                    logging.info(f"action: EOF message processed | request_id: {message.request_id} | type: {message.type}")
                    self._process_pending_clients(message, data_output_middleware)
                    self._send_eof(message, data_output_middleware)
                return
            
            self._process_items(message)
            request_id = message.request_id
            self.snapshot_interval.setdefault(request_id, 0)
            self.snapshot_interval[request_id] += 1
            
            if self.snapshot_interval[request_id] >= SNAPSHOT_INTERVAL:
                logging.info(f"action: snapshot_interval_reached | request_id: {message.request_id} | interval: {SNAPSHOT_INTERVAL}")
                self.snapshot_interval[request_id] = 0
                self.joiner_storage.save_state(message.request_id)
            else:
                self.joiner_storage.append_state(message.request_id)
            
        items_input_queue.start_consuming(__on_items_message__)
     
   
    @abstractmethod   
    def _process_items(self, message: Message):
        pass
    
    @abstractmethod
    def _process_items_to_join(self, message: Message, data_output_middleware: MessageMiddleware):
        pass
    
    @abstractmethod
    def _process_pending_clients(self, message: Message, data_output_middleware: MessageMiddleware):
        pass
    
    @abstractmethod
    def _join(self, request_id, item) -> bool:
        pass
    
    @abstractmethod
    def _send_message(self, data_output_middleware: MessageMiddleware, message: Message):
        pass
    
    def _process_items_to_join_by_message_type(self, message: Message, data_output_middleware: MessageMiddleware, message_type):
        try:
            items = message.process_message()
            state = self.state_storage.get_state(message.request_id)
            pending_results = state.get("pending_results", [])
            
            chunk = ''
            
            for item in items:
                new_chunk = self._join(message.request_id, item)
                logging.info(f"{new_chunk}")
                if new_chunk:
                    chunk += new_chunk
                    logging.info(f"action: item joined successfully | request_id: {message.request_id}")
                    continue   
                else: 
                    pending_results.append(item)
                    logging.info(f"action: no_matching_join_item | request_id: {message.request_id}")
                    
            if chunk:
                new_msg_num = self._get_new_msg_num(message.request_id)
                out_msg = Message(message.request_id, message_type, new_msg_num, chunk, self.node_id)
                self._send_message(data_output_middleware, out_msg)
                logging.info(f"action: results sent after join | request_id: {message.request_id} | items_count: {len(chunk)}")
                
            state["pending_results"] = pending_results
            self.state_storage.data_by_request[message.request_id] = state
            
        except Exception as e:
            logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")
            return
        
    def _process_pending_clients_by_message_type(self, message: Message, data_output_middleware: MessageMiddleware, message_type):
        try:
            state = self.state_storage.get_state(message.request_id)
            pending_results = state.get("pending_results", [])
            chunk = ''
            
            for item in pending_results:
                new_chunk = self._join(message.request_id, item)
                if new_chunk:
                    chunk += new_chunk
                    
            if chunk:
                new_msg_num = self._get_new_msg_num(message.request_id)
                msg = Message(message.request_id, message_type, new_msg_num, chunk, self.node_id)
                self._send_message(data_output_middleware, msg)
                logging.info(f"action: Pending Q2 results sent after join | request_id: {message.request_id} | items_count: {len(pending_results)}")
            else: 
                logging.info(f"action: no pending results to send after join | request_id: {message.request_id}")
        except Exception as e:
            logging.error(f"action: error processing items to join | request_id: {message.request_id} | error: {str(e)}")