import os
from os.path import join as _j
from pathlib import Path
import pytest
import sqlite3
from typing import List, Dict, Tuple
import functools

from bellastore.utils.scan import Scan
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable
from bellastore.database.storage import StorageTable
from bellastore.filesystem.storage import Storage
    

# CREATING SCANS TO BE PUT INTO OUR FILESYSTE
def create_scans(path: Path, amount = 4) -> List[Scan]:
    scans = []
    for i in range(amount):
        p = path / f"scan_{i}.ndpi"
        p.write_text(f"Content of scan_{i}.ndpi", encoding="utf-8")
        scan = Scan(str(p))
        scan.state.move_forward()
        scans.append(scan)
    return scans

@pytest.fixture(scope="function")
def scans(tmp_path):
    yield create_scans(tmp_path, 4)

# FILESYSTEMS
# The idea here is to set up filesystems with different layout

# everything starts at a temporary root
@pytest.fixture(scope="function")
def root_dir(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    yield root_dir

# blueprint fs
class Fs:
    def __init__(self, root_dir):
        '''
        A Fs only contains a storage and an ingress (and of course a root).
        So this serves as a blueprint for all subsequent filesystems
        '''
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        os.makedirs(self.storage_dir, exist_ok = True)
        self.ingress_dir = _j(root_dir, "ingress")
        os.makedirs(self.ingress_dir, exist_ok = True)

    @staticmethod
    def _get_files(dir):
        files = Path(dir).rglob("*")
        file_paths = list(files)
        file_paths = [str(file) for file in file_paths if file.is_file()]
        return file_paths

    
    def add_scan_to_ingress(self, scan: Scan):
        # Moving to ingress means hashing
        scan.hash_scan()
        scan.move(self.ingress_dir)
    def add_scan_to_storage(self, scan: Scan):
        self.add_scan_to_ingress(scan)
        target_dir = os.path.join(self.storage_dir, scan.hash)
        scan.move(target_dir)
    def add_scans_to_ingress(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress(scan)
    def add_scans_to_storage(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage(scan)
    
    def get_files_from_ingress(self):
        print(f"The files in {self.ingress_dir} are")
        return self._get_files(self.ingress_dir)
    def get_files_from_storage(self):
        print(f"The files in {self.storage_dir} are")
        return self._get_files(self.storage_dir)
    
    def __str__(self):
        ingress = self.get_files_from_ingress()
        storage = self.get_files_from_storage()
        return(f"Ingress: {ingress}\n Storage: {storage}")

# the filesystem layouts to be tested against
@pytest.fixture(scope="function")
def ingress_only_fs(root_dir, scans):
    '''
    Contains all slides in ingress and none in storage
    '''
    fs = Fs(root_dir)
    fs.add_scans_to_ingress(scans)
    return fs

@pytest.fixture(scope="function")
def storage_only_fs(root_dir, scans):
    '''
    Contains all slides in storage and none in ingress
    '''
    fs = Fs(root_dir)
    fs.add_scans_to_storage(scans)
    return fs 

@pytest.fixture(scope="function")
def classic_fs(root_dir, scans):
    '''
    Contains half the slides in ingress and half already in storage
    '''
    fs = Fs(root_dir)
    fs.add_scans_to_ingress(scans[0:2])
    fs.add_scans_to_storage(scans[2:4])
    return fs 


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

@pytest.fixture(scope="function")
def empty_db(root_dir):
    db = Db(root_dir, 'scans.sqlite')
    return db

@pytest.fixture(scope="function")
def ingress_only_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    db.add_scans_to_ingress_db(scans)
    return db

@pytest.fixture(scope="function")
def storage_only_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    db.add_scans_to_storage_db(scans)
    return db

@pytest.fixture(scope="function")
def classic_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    db.add_scans_to_ingress_db(scans[0:2])
    db.add_scans_to_storage_db(scans[2:4])
    return db



    
     

    

# # Here we set up databases at different stages in the storage process, e.g.
# # empty or already filled with some scans
# @pytest.fixture(scope="function")
# def empty_scan_db(root_dir):
#     '''
#     Just an empty scan database
#     '''
#     scan_db = ScanDatabase(root_dir)
#     yield scan_db

# @pytest.fixture(scope="function")
# def ingress_scan_db(root_dir, ingress_fs):
#     '''
#     A database that has the files from ingress already in its ingress table, but
#     neither in the storage db nor in storage
#     '''
#     ingress_db = ScanDatabase(root_dir)
#     ingress_table = IngressTable(ingress_db.sqlite_path)
#     ingress_table.write(ingress_fs.files)
#     yield ingress_db

# @pytest.fixture(scope="function")
# def storage_scan_db(storage_fs, hashed_scans, storage_scans):
#     '''
#     A database that is already filled with 2 scans (in ingress and storage table)
#     '''
#     scan_db = ScanDatabase(storage_fs.root_dir)
#     ingress_table = IngressTable(scan_db.sqlite_path)
#     ingress_table.write_many(hashed_scans)
#     storage_table = StorageTable(scan_db.sqlite_path)
#     # here it curcial to give the storage scans the correct path, becuase they are in a non-hashed folder
#     for scan in storage_scans:
#         scan.path = os.path.join(storage_fs.storage_dir, scan.hash, scan.filename)
#     storage_table.write_many(storage_scans)
#     yield scan_db





# HELPERS

def execute_sql(path: str, query: str, params: tuple = ()) -> list:
    """Helper function to execute SQL queries."""
    db_connection = sqlite3.connect(path)
    cursor = db_connection.cursor()
    cursor.execute(query, params)
    db_connection.commit()
    return cursor.fetchall()

def get_tables(path: str):
    tables = execute_sql(path, "SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in tables]
    return tables

def get_scheme(path: str, table: str):
    scheme = execute_sql(path, f"PRAGMA table_info({table});")
    return scheme