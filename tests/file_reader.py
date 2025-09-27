from client.file_reader import FileReader

def test_file_reader():
    file = 'data/menu_items.csv'
    
    fileReader = FileReader(file, 10000)
    
    chunk = fileReader.get_chunk()
    assert len(chunk) > 0
    
    