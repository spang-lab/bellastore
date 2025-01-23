import sqlite3
from typing import List, Dict, Tuple

class BaseTable():
    def __init__(self, sqlite_path, name):
        self.sqlite_path = sqlite_path
        self.name = name
    
    def read_all(self):
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.name}")
        data = cursor.fetchall()
        conn.commit()
        conn.close()
        return data
    
    def read_all_of_column(self, column: str):
        # Get existing hashes from the storage table
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT {column} FROM {self.name}")
        entries = cursor.fetchall()
        entries = [row[0] for row in entries]
        conn.close()
        return entries
    
    def read_all_of_columns(self, columns: List[str]):
        # Get existing hashes from the storage table
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT {', '.join(columns)} FROM {self.name}")
        entries = cursor.fetchall()
        conn.close()
        return entries        
    
    # can be used to read specific line according to hash
    def read(self):
        pass

    def read_many(self):
        pass

    def delete(self):
        pass

    def write(self):
        pass
