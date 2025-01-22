import pytest
import sqlite3
from os.path import join as _j

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase

def test_db_initialization(filesystem):
    scan_db = ScanDatabase(filesystem)
    tables = get_tables(scan_db.sqlite_path)
    assert not scan_db.sqlite_path == None
    assert 'ingress' in tables
    assert 'storage' in tables
    for table in tables:
        scheme = get_scheme(scan_db.sqlite_path, table)
        if table == 'ingress':
            assert [(0, 'hash', 'TEXT', 0, None, 0), (1, 'filepath', 'TEXT', 0, None, 0), (2, 'filename', 'TEXT', 0, None, 0)] == scheme
        elif table == 'storage':
            assert [(0, 'hash', 'TEXT', 1, None, 1), (1, 'filepath', 'TEXT', 0, None, 0), (2, 'filename', 'TEXT', 0, None, 0)] == scheme
        else:
            # this asserts that the hash column fulfills:
            # 1) its the first column in the table
            # 2) its type is TEXT
            # 3) its not Null
            # 4) there is no default
            # 5) it is the primary key
            assert (0, 'hash', 'TEXT', 1, None, 1) in scheme



# def test_database_initialization(filesystem):
#     """Initializes the database to the current storage"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = True)

# def test_ingress_to_storage():
#     """Tests the full machinery for a new ingress folder"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = False)
#     dbase.ingress_to_storage(_j(filesystem, "ingress"))
