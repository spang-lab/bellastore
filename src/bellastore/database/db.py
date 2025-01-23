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
    def add_scan_to_ingress_db(self, cursor, scan: Scan, rec = False):
        # rec tells us if the scan is already recorded in storage, so we do not
        # have to physically move the file around
        if not rec:
            self._add_scan_to_ingress(scan)
        print(f"Recording scan in ingress")
        cursor.execute(
            f"INSERT INTO ingress (hash, filepath, filename) VALUES (?, ?, ?)",
            (scan.hash, scan.path, scan.scanname)
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
            return
        existing_storage_entries = self.get_entries_from_storage_db()
        existing_storage_hashes = [entry[0] for entry in existing_storage_entries]
        if scan.hash in existing_storage_hashes:
            print(f'Scan already recorded in the storage table, so scan will be only recorded in ingress table and then deleted.')
            self.add_scan_to_ingress_db(scan, rec = True)
            os.remove(scan.path)
            return
        # In this case the scan is not recorded, thus also not in storage so we run the whole pipeline
        # This will also automatically care about the storage backend recursively
        self.add_scan_to_storage_db(scan)


        



        

        
    @sqlite_connection  
    def insert_many():
        pass

    @sqlite_connection 
    def _read_all(self, cursor, table_name: str):
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        return data

    def get_entries_from_ingress_db(self, verbose = False):
        if verbose:
            print(f"The files in the ingress table are:\n")
        return self._read_all('ingress')
    
    def get_entries_from_storage_db(self, verbose = False):
        if verbose:
            print(f"The files in the storage table are:\n")
        return self._read_all('storage')
    
    def __str__(self):
        ingress = self.get_entries_from_ingress_db()
        storage = self.get_entries_from_storage_db()
        return(f"Ingress DB: {ingress}\n Storage DB: {storage}")