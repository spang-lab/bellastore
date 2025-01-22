import sqlite3

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
    
    # can be used to read specific line according to hash
    def read(self):
        pass

    def read_many(self):
        pass

    def delete(self):
        pass

    def write(self):
        pass
