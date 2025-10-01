from client.common.file_reader import FileReader

def __test_file_reader(file):
    file_reader = FileReader(file, 10000)
    
    chunk = file_reader.get_chunk()
    assert len(chunk) > 0
    
    items = chunk.splitlines()
    
    assert file_reader.is_eof == True
    file_reader.close()
    return items
    

def test_file_reader_menu_items():
    file = 'test/data/menu_items.csv'
    menu_items = __test_file_reader(file)
    
    for index, item in enumerate(menu_items):
        parts = item.split(',')
        assert len(parts) == 7
        assert parts[0] == str(index + 1)
        

def test_file_reader_stores():
    file = 'test/data/stores.csv'
    orders = __test_file_reader(file)
    
    for index, order in enumerate(orders):
        parts = order.split(',')
        assert len(parts) == 8
        assert parts[0] == str(index + 1)