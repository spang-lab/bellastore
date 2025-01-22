import pytest
import sqlite3
from os.path import join as _j

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable

def test_db_initialization(empty_scan_db):
    tables = get_tables(empty_scan_db.sqlite_path)
    assert not empty_scan_db.sqlite_path == None
    assert 'ingress' in tables
    assert 'storage' in tables
    for table in tables:
        scheme = get_scheme(empty_scan_db.sqlite_path, table)
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

def test_ingress_write(ingress_fs, empty_scan_db, hashed_scans):
    ingress_table = IngressTable(empty_scan_db.sqlite_path)
    ingress_table.write(ingress_fs.files)
    ingress_entries = ingress_table.read_all()
    ingress_entries_hashes = {ingress_entry[0] for ingress_entry in ingress_entries}
    ingress_entries_paths = {ingress_entry[1] for ingress_entry in ingress_entries}
    assert {hashed_scans[0].hash, hashed_scans[1].hash} == ingress_entries_hashes
    assert {hashed_scans[0].path, hashed_scans[1].path} == ingress_entries_paths


    
    








# def test_database_initialization(filesystem):
#     """Initializes the database to the current storage"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = True)

# def test_ingress_to_storage():
#     """Tests the full machinery for a new ingress folder"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = False)
#     dbase.ingress_to_storage(_j(filesystem, "ingress"))
