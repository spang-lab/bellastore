import os
from os.path import join as _j
from pathlib import Path
import pytest
import sqlite3
from typing import List, Dict, Tuple

from bellastore.utils.scan import Scan
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable
from bellastore.database.storage import StorageTable
from bellastore.filesystem.storage import Storage
    

# CREATING SCANS FOR FURTHER USE
def create_scans(path: Path, amount = 4) -> List[Scan]:
    scans = []
    for i in range(amount):
        p = path / f"scan_{i}.ndpi"
        p.write_text(f"Content of scan_{i}.ndpi", encoding="utf-8")
        scans.append(Scan(str(p)))
    return scans

@pytest.fixture(scope="function")
def scans(tmp_path):
    yield create_scans(tmp_path, 4)

# FILESYSTEMS

# The idea here is to set up filesystems with different layout, e.g. where the slides are located

# everything starts at a temporary root
@pytest.fixture(scope="function")
def root_dir(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    yield root_dir

# class Fs:
#     def __init__(self, root_dir):
#         self.root_dir = root_dir
#         self.storage_dir = _j(root_dir, "storage")
#         self.ingress_dir = _j(root_dir, "ingress")

#     def __add_scan(scan, target_dir):

    
#     def add_scan_to_ingress():
    
#     def add_scan_to_storage():
#         self.scan_1_dir = _j(self.ingress_dir, "scan_1")
#         self.scan_2_dir = _j(self.ingress_dir, "scan_2")
#         # Define directory structure
#         dirs = [
#             self.storage_dir,
#             self.ingress_dir,
#             self.scan_1_dir,
#             self.scan_2_dir
#         ]
#         # Create all directories
#         for dir_path in dirs:
#             os.makedirs(dir_path, exist_ok=True)
        
#         # Create 2 DIFFERENT scans
#         self.scan_1_path = _j(self.scan_1_dir, "test_scan_1.ndpi")
#         with open(self.scan_1_path, 'w') as f:
#             f.write('test_scan_1.ndpi')
#             f.close
#         self.scan_2_path = _j(self.scan_2_dir, "test_scan_2.ndpi")
#         with open(self.scan_2_path, 'w') as f:
#             f.write('test_scan_2.ndpi')
#             f.close
#         self.files = {self.scan_1_path, self.scan_2_path}   


class IngressFs:
    '''
    This is a filesystem that has 2 valid scanner slides in its INGRESS,
    and none in its storage
    '''
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        self.ingress_dir = _j(root_dir, "ingress")
        self.scan_1_dir = _j(self.ingress_dir, "scan_1")
        self.scan_2_dir = _j(self.ingress_dir, "scan_2")
        # Define directory structure
        dirs = [
            self.storage_dir,
            self.ingress_dir,
            self.scan_1_dir,
            self.scan_2_dir
        ]
        # Create all directories
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create 2 DIFFERENT scans
        self.scan_1_path = _j(self.scan_1_dir, "test_scan_1.ndpi")
        with open(self.scan_1_path, 'w') as f:
            f.write('test_scan_1.ndpi')
            f.close
        self.scan_2_path = _j(self.scan_2_dir, "test_scan_2.ndpi")
        with open(self.scan_2_path, 'w') as f:
            f.write('test_scan_2.ndpi')
            f.close
        self.files = {self.scan_1_path, self.scan_2_path}

@pytest.fixture(scope="function")
def ingress_fs(root_dir):
    ingress_fs = IngressFs(root_dir)
    yield ingress_fs

@pytest.fixture(scope="function")
def hashed_scans(ingress_fs):
    scan_1 = Scan(ingress_fs.scan_1_path)
    scan_1.state.move_forward()
    scan_1.hash = scan_1.hash_scan()
    scan_1.state.move_forward()
    scan_2 = Scan(ingress_fs.scan_2_path)
    scan_2.state.move_forward()
    scan_2.hash = scan_2.hash_scan()
    scan_2.state.move_forward()
    return [scan_1, scan_2]


class IngressFsFresh:
    '''
    This is a filesystem that has 2 valid scanner slides in its INGRESS,
    and none in its storage
    '''
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        self.ingress_dir = _j(root_dir, "ingress")
        self.scan_1_dir = _j(self.ingress_dir, "scan_3")
        self.scan_2_dir = _j(self.ingress_dir, "scan_4")
        # Define directory structure
        dirs = [
            self.storage_dir,
            self.ingress_dir,
            self.scan_1_dir,
            self.scan_2_dir
        ]
        # Create all directories
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create 2 DIFFERENT scans
        self.scan_1_path = _j(self.scan_1_dir, "test_scan_3.ndpi")
        with open(self.scan_1_path, 'w') as f:
            f.write('test_scan_3.ndpi')
            f.close
        self.scan_2_path = _j(self.scan_2_dir, "test_scan_4.ndpi")
        with open(self.scan_2_path, 'w') as f:
            f.write('test_scan_4.ndpi')
            f.close
        self.files = {self.scan_1_path, self.scan_2_path}

@pytest.fixture(scope="function")
def ingress_fs_fresh(root_dir):
    ingress_fs = IngressFsFresh(root_dir)
    yield ingress_fs

@pytest.fixture(scope="function")
def hashed_scans_fresh(ingress_fs_fresh):
    scan_1 = Scan(ingress_fs_fresh.scan_1_path)
    scan_1.state.move_forward()
    scan_1.hash = scan_1.hash_scan()
    scan_1.state.move_forward()
    scan_2 = Scan(ingress_fs_fresh.scan_2_path)
    scan_2.state.move_forward()
    scan_2.hash = scan_2.hash_scan()
    scan_2.state.move_forward()
    return [scan_1, scan_2]


class StorageFs:
    '''
    This is a filesystem that has 2 valid scanner slides in its STORAGE,
    and none in its ingress
    '''
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        self.ingress_dir = _j(root_dir, "ingress")
        self.scan_1_dir = _j(self.storage_dir, "scan_1")
        self.scan_2_dir = _j(self.storage_dir, "scan_2")
        # Define directory structure
        dirs = [
            self.storage_dir,
            self.ingress_dir,
            self.scan_1_dir,
            self.scan_2_dir
        ]
        # Create all directories
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create 2 DIFFERENT scans
        self.scan_1_path = _j(self.scan_1_dir, "test_scan_1.ndpi")
        with open(self.scan_1_path, 'w') as f:
            f.write('test_scan_1.ndpi')
            f.close
        self.scan_2_path = _j(self.scan_2_dir, "test_scan_2.ndpi")
        with open(self.scan_2_path, 'w') as f:
            f.write('test_scan_2.ndpi')
            f.close
        self.files = {self.scan_1_path, self.scan_2_path}

@pytest.fixture(scope="function")
def storage_fs(root_dir):
    storage_fs = StorageFs(root_dir)
    yield storage_fs

@pytest.fixture(scope="function")
def storage_from_storage_fs(storage_fs):
    yield Storage(storage_fs.storage_dir)

@pytest.fixture(scope="function")
def storage_scans(storage_fs):
    scan_1 = Scan(storage_fs.scan_1_path)
    scan_1.state.move_forward()
    scan_1.hash = scan_1.hash_scan()
    scan_1.state.move_forward()
    # to be in storage
    scan_1.state.move_forward()
    scan_2 = Scan(storage_fs.scan_2_path)
    scan_2.state.move_forward()
    scan_2.hash = scan_2.hash_scan()
    scan_2.state.move_forward()
    scan_2.state.move_forward()
    return [scan_1, scan_2]

class StorageFsFresh:
    '''
    This is a filesystem that has 2 valid scanner slides in its STORAGE,
    and none in its ingress
    '''
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        self.ingress_dir = _j(root_dir, "ingress")
        self.scan_1_dir = _j(self.storage_dir, "scan_3")
        self.scan_2_dir = _j(self.storage_dir, "scan_4")
        # Define directory structure
        dirs = [
            self.storage_dir,
            self.ingress_dir,
            self.scan_1_dir,
            self.scan_2_dir
        ]
        # Create all directories
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create 2 DIFFERENT scans
        self.scan_1_path = _j(self.scan_1_dir, "test_scan_3.ndpi")
        with open(self.scan_1_path, 'w') as f:
            f.write('test_scan_3.ndpi')
            f.close
        self.scan_2_path = _j(self.scan_2_dir, "test_scan_4.ndpi")
        with open(self.scan_2_path, 'w') as f:
            f.write('test_scan_4.ndpi')
            f.close
        self.files = {self.scan_1_path, self.scan_2_path}

@pytest.fixture(scope="function")
def storage_fs_fresh(root_dir):
    storage_fs = StorageFsFresh(root_dir)
    yield storage_fs

@pytest.fixture(scope="function")
def storage_from_storage_fs_fresh(storage_fs_fresh):
    yield Storage(storage_fs_fresh.storage_dir)

@pytest.fixture(scope="function")
def storage_scans_fresh(storage_fs_fresh):
    scan_1 = Scan(storage_fs_fresh.scan_1_path)
    scan_1.state.move_forward()
    scan_1.hash = scan_1.hash_scan()
    scan_1.state.move_forward()
    # to be in storage
    scan_1.state.move_forward()
    scan_2 = Scan(storage_fs_fresh.scan_2_path)
    scan_2.state.move_forward()
    scan_2.hash = scan_2.hash_scan()
    scan_2.state.move_forward()
    scan_2.state.move_forward()
    return [scan_1, scan_2]


# DATABSES

# Here we set up databases at different stages in the storage process, e.g.
# empty or already filled with some scans
@pytest.fixture(scope="function")
def empty_scan_db(root_dir):
    '''
    Just an empty scan database
    '''
    scan_db = ScanDatabase(root_dir)
    yield scan_db

@pytest.fixture(scope="function")
def ingress_scan_db(root_dir, ingress_fs):
    '''
    A database that has the files from ingress already in its ingress table, but
    neither in the storage db nor in storage
    '''
    ingress_db = ScanDatabase(root_dir)
    ingress_table = IngressTable(ingress_db.sqlite_path)
    ingress_table.write(ingress_fs.files)
    yield ingress_db

@pytest.fixture(scope="function")
def storage_scan_db(storage_fs, hashed_scans, storage_scans):
    '''
    A database that is already filled with 2 scans (in ingress and storage table)
    '''
    scan_db = ScanDatabase(storage_fs.root_dir)
    ingress_table = IngressTable(scan_db.sqlite_path)
    ingress_table.write_many(hashed_scans)
    storage_table = StorageTable(scan_db.sqlite_path)
    # here it curcial to give the storage scans the correct path, becuase they are in a non-hashed folder
    for scan in storage_scans:
        scan.path = os.path.join(storage_fs.storage_dir, scan.hash, scan.filename)
    storage_table.write_many(storage_scans)
    yield scan_db





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