import os
from os.path import join as _j
from pathlib import Path
import pytest
import sqlite3
from typing import List, Dict, Tuple
import functools

from bellastore.filesystem.fs import Fs
from bellastore.utils.scan import Scan

# DATABSES
def sqlite_connection(func):
    '''
    This decorator is genious
    '''
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            try:
                result = func(self, cursor, *args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise e
    return wrapper

class Db(Fs):
    def __init__(self, root_dir, filename):
        super().__init__(root_dir)
        self.filename = filename
        self.sqlite_path = os.path.join(self.storage_dir, self.filename)
        self._initialize_db()

    @sqlite_connection
    def _initialize_db(self, cursor):
        cursor.execute('''
        CREATE TABLE ingress (
            hash TEXT,
            filepath TEXT,
            filename TEXT,
            UNIQUE(hash, filepath, filename)
        )
        ''')
        cursor.execute('''
        CREATE TABLE storage (
            hash TEXT NOT NULL PRIMARY KEY,
            filepath TEXT,
            filename TEXT,
            scanname TEXT,
            FOREIGN KEY(hash) REFERENCES ingress(hash)
        )
        ''')

    @sqlite_connection  
    def add_scan_to_ingress_db(self, cursor, scan: Scan):
        self.add_scan_to_ingress(scan)
        cursor.execute(
            f"INSERT INTO ingress (hash, filepath, filename) VALUES (?, ?, ?)",
            (scan.hash, scan.path, scan.scanname)
        )
    def add_scans_to_ingress_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress_db(scan)

    @sqlite_connection  
    def add_scan_to_storage_db(self, cursor, scan: List[Scan]):
        # This is super important
        self.add_scan_to_storage(scan)
        cursor.execute(f"""
            INSERT INTO storage (hash, filepath, filename, scanname) 
            VALUES (?, ?, ?, ?)
            """, (scan.hash, scan.path, scan.filename, scan.scanname))

    def add_scans_to_storage_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage_db(scan)

    @sqlite_connection 
    def _read_all(self, cursor, table_name: str):
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        return data

    def get_entries_from_ingress_db(self):
        print(f"The files in the ingress table are:\n")
        return self._read_all('ingress')
    
    def get_entries_from_storage_db(self):
        print(f"The files in the storage table are:\n")
        return self._read_all('storage')
    
    def __str__(self):
        ingress = self.get_entries_from_ingress_db()
        storage = self.get_entries_from_storage_db()
        return(f"Ingress DB: {ingress}\n Storage DB: {storage}")