from client.common.file_reader import FileReader
from packages.messages.message import Message
from packages.messages.constants import MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_TRANSACTION_ITEMS

def __test_message(file, message_type):
    fileReader = FileReader(file, 10000)
    chunk = ''
    while not fileReader.is_eof:
        chunk += fileReader.get_chunk()
    
    message =  Message(1,message_type, 1, chunk)
    serialized_message = message.__serialize__()
    message_length = int.from_bytes(serialized_message[0:4], byteorder='big')
    assert message_length == len(serialized_message) - 4
    
    serialized_message = serialized_message[4:]
    new_message = Message.__deserialize__(serialized_message)
    assert new_message.type == message_type
    assert new_message.request_id == 1
    assert new_message.msg_num == 1
    
    items = message.proccess_message()
    return items

def test_message_menu_items():
    file = 'test/data/menu_items.csv'
    
    menu_items_list = __test_message(file, MESSAGE_TYPE_MENU_ITEMS)
    assert len(menu_items_list) == 8
    assert menu_items_list[0].id == 1
    assert menu_items_list[0].name == "Espresso"
    assert menu_items_list[0].price == 6.0
    
    
def test_message_stores():
    file = 'test/data/stores.csv'
    
    stores = __test_message(file, MESSAGE_TYPE_STORES)
    assert len(stores) == 10
    assert stores[0].id == 1
    assert stores[0].name == "G Coffee @ USJ 89q"
    assert stores[9].id == 10
    assert stores[9].name == "G Coffee @ Taman Damansara"
    
def test_message_transaction_items():
    file = 'test/data/transaction_items_202503.csv'
    
    transaction_items = __test_message(file, MESSAGE_TYPE_TRANSACTION_ITEMS)
    assert len(transaction_items) == 165
    
    assert transaction_items[9].transaction_id == '64d81604-38c8-4e04-977c-80d8090df7ea'
    assert transaction_items[9].item_id == 8
    assert transaction_items[9].created_at == '2025-03-01 11:20:30'