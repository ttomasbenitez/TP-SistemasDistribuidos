
from pkg.message.message import Message

class FormatResults:
    
    def __init__(self, message):
        self.message = message
        
    def format_chunk(self):
        items = self.message.process_message()
        formatted_data = {}
        for item in items:
            formatted_data[item.transaction_id] = item.get_final_amount()
            
        return formatted_data
            
    
    