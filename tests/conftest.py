import os
from os.path import join as _j
import pytest
import sqlite3
from typing import Generator

from bellastore.utils.scan import Scan
    


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


class TestFs:
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        self.ingress_dir = _j(root_dir, "ingress")
        self.scan_1_dir = _j(self.ingress_dir, "scan_1")
        self.scan_2_dir = _j(self.ingress_dir, "scan_2")
        # Define directory structure
        dirs = [
            self.storage_dir,
            self.scan_1_dir,
            self.scan_2_dir
        ]
        # Create all directories
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create 2 DIFFERENT scans
        self.scan_1_path = _j(self.scan_1_dir, "test_scan_1.ndpi")
        with open(self.scan_1_path, 'w') as f:
            f.write('scan_1')
            f.close
        self.scan_2_path = _j(self.scan_2_dir, "test_scan_2.ndpi")
        with open(self.scan_2_path, 'w') as f:
            f.write('scan_2')
            f.close

@pytest.fixture(scope="session")
def fs(root_dir):
    test_fs = TestFs(root_dir)
    yield test_fs

@pytest.fixture(scope="session")
def hashed_scans(fs):
    scan_1 = Scan(fs.scan_1_path)
    scan_1.state.move_forward()
    scan_1.hash = scan_1.hash_scan()
    scan_1.state.move_forward()
    scan_2 = Scan(fs.scan_2_path)
    scan_2.state.move_forward()
    scan_2.hash = scan_2.hash_scan()
    scan_2.state.move_forward()
    return [scan_1, scan_2]


# @pytest.fixture
# def db_connection(filesystem):
#     conn = sqlite3.connect(_j(filesystem,"scans.sqlite"))
#     conn.execute("PRAGMA foreign_keys = ON")
#     yield conn
#     conn.close()

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




