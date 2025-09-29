from client.common.file_reader import FileReader
from pkg.message.message import Message
from pkg.message.constants import MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_USERS
from pkg.message.utils import parse_date

def __test_message(file, message_type):
    fileReader = FileReader(file, 10000)
    chunk = ''
    while not fileReader.is_eof:
        chunk += fileReader.get_chunk()
    
    message =  Message(1,message_type, 1, chunk)
    serialized_message = message.serialize()
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
    assert transaction_items[9].created_at == parse_date('2025-03-01 11:20:30')
    
    # test item id null values
    assert transaction_items[0].transaction_id == '017c28a0-e36d-4a2e-be96-5ca5c608d0be'
    assert transaction_items[0].item_id == None
    assert transaction_items[0].created_at.year == 2025
    assert transaction_items[0].created_at.month == 3
    
def test_message_transactions():
    file = 'test/data/transactions_202503.csv'
    
    transaction = __test_message(file, MESSAGE_TYPE_TRANSACTIONS)
    assert len(transaction) == 258
    
    assert transaction[257].transaction_id == 'd13b4c92-d73c-4365-afc4-73e243fedff6'
    assert transaction[257].store_id == 6
    assert transaction[257].user_id == 1051249
    assert transaction[257].final_amount == 19.0
    assert transaction[257].created_at == parse_date('2025-03-01 15:55:32')
    assert transaction[257].created_at.year == 2025
    assert transaction[257].created_at.month == 3
    
    # test store id null values
    assert transaction[0].transaction_id == '2e0b6369-f809-4de3-a2b5-eb932efe2f7a'
    assert transaction[0].store_id == None
    assert transaction[0].user_id == 1454816
    assert transaction[0].final_amount == 40
    assert transaction[0].created_at == parse_date('2025-03-01 07:00:04')
    
    # test user id null values and final amount with decimal values
    assert transaction[1].transaction_id == 'c712125c-e578-4376-b43e-466e1ac5ee60'
    assert transaction[1].store_id == 4
    assert transaction[1].user_id == None
    assert transaction[1].final_amount == 38.0
    assert transaction[1].created_at == parse_date('2025-03-01 15:45:44')

    # test final amount null values
    assert transaction[2].transaction_id == 'd947216d-4c88-4f9e-8ed9-7c47bea08baa'
    assert transaction[2].store_id == 8
    assert transaction[2].user_id == 499937
    assert transaction[2].final_amount == None
    assert transaction[2].created_at == parse_date('2025-03-01 15:45:47')
 
 
def test_users():
    file = 'test/data/users_202503.csv'
    
    users = __test_message(file, MESSAGE_TYPE_USERS)
    assert len(users) == 14
    assert users[0].id == 1472011
    assert users[0].birthdate == "1966-09-05"
    assert users[0].registered_at.year == 2025
    
    