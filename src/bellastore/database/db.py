import os
from os.path import join as _j
import sqlite3
from typing import List
import functools
import pandas as pd

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
    def __init__(self, root_dir, ingress_dir, filename):
        super().__init__(root_dir, ingress_dir)
        self.filename = filename
        self.sqlite_path = os.path.join(self.storage_dir, self.filename)
        self._initialize_db()

    @sqlite_connection
    def _table_exists(self, cursor, table_name: str):
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table_name, ))
        result = cursor.fetchall()
        if result:
            return True
        else:
            False

    @sqlite_connection
    def _initialize_db(self, cursor):
        if not self._table_exists('ingress'):
            cursor.execute('''
            CREATE TABLE ingress (
                hash TEXT,
                filepath TEXT,
                filename TEXT,
                UNIQUE(hash, filepath, filename)
            )
            ''')
        if not self._table_exists('storage'):
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
    def add_scan_to_ingress_db(self, cursor, scan: Scan, rec = False):
        # rec tells us if the scan is already recorded in storage, so we do not
        # have to physically move the file around
        if not rec:
            self._add_scan_to_ingress(scan)
        print(f"Recording scan in ingress")
        cursor.execute(
            f"INSERT INTO ingress (hash, filepath, filename) VALUES (?, ?, ?)",
            (scan.hash, scan.path, scan.filename)
        )
    def add_scans_to_ingress_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress_db(scan)

    @sqlite_connection  
    def add_scan_to_storage_db(self, cursor, scan: List[Scan]):
        self.add_scan_to_ingress_db(scan)
        # This is super important
        self.add_scan_to_storage(scan)
        print(f"Recording scan in storage")
        cursor.execute(f"""
            INSERT INTO storage (hash, filepath, filename, scanname) 
            VALUES (?, ?, ?, ?)
            """, (scan.hash, scan.path, scan.filename, scan.scanname))

    def add_scans_to_storage_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage_db(scan)

    def insert(self, scan: Scan):
        '''
        This is where all comes together
        '''
        scan.hash_scan()
        print(f"\nStarting insert pipeline for {scan.hash}:")
        existing_ingress_entries = self.get_entries_from_ingress_db()
        if (scan.hash, scan.path, scan.scanname) in existing_ingress_entries:
            print(f'Scan already recorded in the ingress table and thus scan will be deleted.')
            os.remove(scan.path)
            scan.path = None
            return
        existing_storage_entries = self.get_entries_from_storage_db()
        existing_storage_hashes = [entry[0] for entry in existing_storage_entries]
        if scan.hash in existing_storage_hashes:
            print(f'Scan already recorded in the storage table, so scan will be only recorded in ingress table and then deleted.')
            self.add_scan_to_ingress_db(scan, rec = True)
            print(f'Deleting scan')
            os.remove(scan.path)
            scan.path = None
            return
        # In this case the scan is not recorded, thus also not in storage so we run the whole pipeline
        # This will also automatically care about the storage backend recursively
        self.add_scan_to_storage_db(scan)
    
    def insert_many(self, scans: List[Scan]):
        for scan in scans:
            self.insert(scan)



    @sqlite_connection 
    def _read_all(self, cursor, table_name: str):
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        return data
      
    def _read_all_pd(self, table_name: str):
        conn = sqlite3.connect(self.sqlite_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df


    def get_entries_from_ingress_db(self):
        return self._read_all('ingress')
    
    def get_entries_from_storage_db(self):
        return self._read_all('storage')

    
    def __str__(self):
        # TODO: print_tree method should return a string to be passed to the return
        print("\n")
        self.print_tree()
        ingress_df = self._read_all_pd('ingress')
        storage_df = self._read_all_pd('storage')
        return(f"Ingress DB:\n {ingress_df.to_string()}\n Storage DB:\n {storage_df.to_string()}\n")