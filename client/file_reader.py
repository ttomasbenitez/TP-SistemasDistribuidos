import csv

class FileReader:
    """
    Clase para leer un archivo CSV en chunks limitados por tamaño en bytes.
    """

    def __init__(self, file_path, max_chunk_size):
        """
        Inicializa el lector de archivos.

        :param file_path: Ruta al archivo CSV.
        :param max_chunk_size: Tamaño máximo del chunk en bytes.
        """
        try:
            self.file = open(file_path, 'r')
        except FileNotFoundError:
            raise FileNotFoundError(f"El archivo '{file_path}' no existe.")
        self.reader = csv.reader(self.file, delimiter=',')
        self.max_chunk_size = max_chunk_size
        self.buffered_line = None
        self.is_eof = False
        self.header = next(self.reader, None)
        

    def get_chunk(self):
        """
        Lee un chunk del archivo, limitado por el tamaño máximo en bytes.

        :return: Lista de filas (listas) que componen el chunk.
        """
        chunk = []
        current_size = 0

        if self.buffered_line:
            buffered_size = len(str(self.buffered_line).encode('utf-8'))
            chunk.append(self.buffered_line)
            current_size += buffered_size
            self.buffered_line = None

        for row in self.reader:
            row_size = len(str(row).encode('utf-8'))
            if current_size + row_size > self.max_chunk_size:
                self.buffered_line = row
                break
            chunk.append(row)
            current_size += row_size
        else:
            self.is_eof = True

        return chunk

    def close(self):
        """
        Cierra el archivo.
        """
        self.file.close()