from client.file_reader import FileReader

def test_file_reader():
    file = 'data/menu_items.csv'
    
    fileReader = FileReader(file, 10000)
    
    chunk = fileReader.get_chunk()
    assert len(chunk) > 0
    
    menu_items = chunk.splitlines()
    assert len(menu_items) == 8
    
    for index, item in enumerate(menu_items):
        parts = item.decode('utf-8').split(',')
        assert len(parts) == 7
        assert parts[0] == str(index + 1)
        
    assert fileReader.is_eof == True
    fileReader.close()

    