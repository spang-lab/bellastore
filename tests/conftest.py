import os
from os.path import join as _j
import pytest
import sqlite3
from typing import Generator

from bellastore.utils.scan import Scan
from bellastore.database.database import ScanDatabase
    


@pytest.fixture(scope="session")
def ndpi_scan(tmp_path_factory):
    '''Create empty ndpi scan called test_scan_ndpi and returns its path'''
    tmp_file_path = _j(tmp_path_factory.mktemp("data"),"test_scan.ndpi")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path

@pytest.fixture(scope="session")
def txt_scan(tmp_path_factory):
    '''Create empty (invalid) txt scan called test_scan_ndpi and returns its path'''
    tmp_file_path = _j(tmp_path_factory.mktemp("data"),"test_scan.txt")
    open(tmp_file_path, 'w').close()

@pytest.fixture(scope="session")
def root_dir(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    yield root_dir
    


class IngressFs:
    '''
    This is a filesystem that has 2 valid scanner slides in its ingress,
    and non in its storage
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

@pytest.fixture(scope="session")
def ingress_fs(root_dir):
    ingress_fs = IngressFs(root_dir)
    yield ingress_fs

@pytest.fixture(scope="session")
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

class StorageFs:
    '''
    This is a filesystem that has 2 valid scanner slides in its storage,
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

@pytest.fixture(scope="session")
def storage_fs(root_dir):
    storage_fs = StorageFs(root_dir)
    yield storage_fs

@pytest.fixture(scope="session")
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

@pytest.fixture(scope="session")
def empty_scan_db(root_dir):
    scan_db = ScanDatabase(root_dir)
    yield scan_db


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




