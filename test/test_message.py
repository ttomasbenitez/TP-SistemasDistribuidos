from client.file_reader import FileReader
from packages.messages.message import Message
from packages.messages.constants import MESSAGE_TYPE_MENU_ITEMS

def test_message_menu_items():
    file = 'data/menu_items.csv'
    
    fileReader = FileReader(file, 10000)
    chunk = fileReader.get_chunk()
    
    message =  Message(1,MESSAGE_TYPE_MENU_ITEMS, 1, chunk)
    serialized_message = message.__serialize__()
    message_length = int.from_bytes(serialized_message[0:4], byteorder='big')
    assert message_length == len(serialized_message) - 4
    
    serialized_message = serialized_message[4:]
    message.__deserialize__(serialized_message)
    assert message.type == MESSAGE_TYPE_MENU_ITEMS
    assert message.request_id == 1
    assert message.msg_num == 1
    
    menu_items_list = message.proccess_message()
    assert len(menu_items_list) == 8
    
    