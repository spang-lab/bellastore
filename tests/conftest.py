import os
import pytest
import sqlite3
    


@pytest.fixture(scope="session")
def ndpi_scan(tmp_path_factory):
    '''Create empty ndpi scan called test_scan_ndpi and returns its path'''
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.ndpi")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path
    os.remove(tmp_file_path)

@pytest.fixture(scope="session")
def txt_scan(tmp_path_factory):
    '''Create empty (invalid) txt scan called test_scan_ndpi and returns its path'''
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.txt")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path

@pytest.fixture(scope="session")
def filesystem(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    storage_dir = os.path.join(root_dir, "storage")
    scan_1_dir = os.path.join(root_dir, "ingress", "scan_1")
    scan_2_dir = os.path.join(root_dir, "ingress", "scan_2")
    # Define directory structure
    dirs = [
        storage_dir,
        scan_1_dir,
        scan_2_dir
    ]
    # Create all directories
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
    scan_1_path = os.path.join(scan_1_dir, "test_scan_1.ndpi")
    open(scan_1_path, 'w').close()  
    scan_2_path = os.path.join(scan_2_dir, "test_scan_2.ndpi")
    open(scan_2_path, 'w').close()
    yield root_dir

# @pytest.fixture
# def db_connection(filesystem):
#     conn = sqlite3.connect(os.path.join(filesystem,"scans.sqlite"))
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




