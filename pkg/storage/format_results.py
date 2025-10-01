
from pkg.message.message import Message

class FormatResults:
    
    def __init__(self, message):
        self.message = message
        
    def pre_process_chunk(self):
        items = self.message.process_message()
        formatted_data = []
        for item in items:
            formatted_data.append(item.pre_process())
            
        return formatted_data
            
    
    