import pytest
import sqlite3
from os.path import join as _j

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable
from bellastore.database.storage import StorageTable

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
            assert [(0, 'hash', 'TEXT', 1, None, 1), (1, 'filepath', 'TEXT', 0, None, 0), (2, 'filename', 'TEXT', 0, None, 0), (3, 'scanname', 'TEXT', 0, None, 0)] == scheme
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
    valid_scans = ingress_table.write(ingress_fs.files)
    # Check if the database is correctly written
    ingress_entries = ingress_table.read_all()
    ingress_entries_hashes = {ingress_entry[0] for ingress_entry in ingress_entries}
    ingress_entries_paths = {ingress_entry[1] for ingress_entry in ingress_entries}
    assert {hashed_scans[0].hash, hashed_scans[1].hash} == ingress_entries_hashes
    assert {hashed_scans[0].path, hashed_scans[1].path} == ingress_entries_paths
    # Check if valid_scans actually corresponds to the scans that are written
    assert {valid_scans[0].path, valid_scans[1].path} == ingress_entries_paths
    assert {valid_scans[0].hash, valid_scans[1].hash} == {hashed_scans[0].hash, hashed_scans[1].hash}
    for scan in valid_scans:
        assert scan.state.get_state() == 'hashed'

def test_storage_write(storage_from_storage_fs, ingress_scan_db):
    storage_table = StorageTable(ingress_scan_db.sqlite_path)
    storage_scans = storage_from_storage_fs.get_existing_slides(ingress_scan_db.sqlite_path)
    storage_table.write(storage_from_storage_fs.path, storage_scans)
    for scan in storage_scans:
        assert scan.state.get_state() == 'storage_db'
    storage_entries = storage_table.read_all()
    storage_entries_paths = {storage_entry[1] for storage_entry in storage_entries}
    storage_entries_hashes = {storage_entry[0] for storage_entry in storage_entries}
    assert {storage_scans[0].path, storage_scans[1].path} == storage_entries_paths
    assert {storage_scans[0].hash, storage_scans[1].hash} == storage_entries_hashes






    
    








# def test_database_initialization(filesystem):
#     """Initializes the database to the current storage"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = True)

# def test_ingress_to_storage():
#     """Tests the full machinery for a new ingress folder"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = False)
#     dbase.ingress_to_storage(_j(filesystem, "ingress"))
