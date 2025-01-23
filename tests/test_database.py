import pytest
import sqlite3
import os
from os.path import join as _j
from pathlib import Path

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable
from bellastore.database.storage import StorageTable

# Just for inspecting the dbs
def test_show_empty_db(empty_db):
    print(str(empty_db))
def test_show_ingress_db(ingress_only_db):
    print(str(ingress_only_db))
def test_show_storage_db(storage_only_db):
    print(str(storage_only_db))
def test_show_classic_db(classic_db):
    print(str(classic_db))

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
    '''
    Write files in ingress to the ingress table
    '''
    ingress_table = IngressTable(empty_scan_db.sqlite_path)
    valid_scans = ingress_table.write_candidate_scans(ingress_fs.files)
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

def test_ingress_write_duplicates(ingress_fs, storage_scan_db):
    '''
    The storage_scan_db already contains the hashed_scans in its tables,
    so nothing should be added to those tables
    '''
    ingress_table = IngressTable(storage_scan_db.sqlite_path)
    valid_scans = ingress_table.write_candidate_scans(ingress_fs.files)
    # Check that no scans are written
    assert not valid_scans
    # Check that ingress is clean
    # TODO: feature
    # assert os.listdir(ingress_fs.ingress_dir) == []

def test_ingress_write_fresh(ingress_fs_fresh, storage_scan_db, hashed_scans_fresh):
    ingress_table = IngressTable(storage_scan_db.sqlite_path)
    valid_scans = ingress_table.write_candidate_scans(ingress_fs_fresh.files)
    # Check if the database is correctly written
    ingress_entries = ingress_table.read_all()
    ingress_entries_hashes = {ingress_entry[0] for ingress_entry in ingress_entries}
    ingress_entries_paths = {ingress_entry[1] for ingress_entry in ingress_entries}
    # the not statements check that the already existing entries in the ingress table are not overwritten
    assert {hashed_scans_fresh[0].hash, hashed_scans_fresh[1].hash}.issubset(ingress_entries_hashes)
    assert not {hashed_scans_fresh[0].hash, hashed_scans_fresh[1].hash} == ingress_entries_hashes
    assert {hashed_scans_fresh[0].path, hashed_scans_fresh[1].path}.issubset(ingress_entries_paths)
    assert not {hashed_scans_fresh[0].path, hashed_scans_fresh[1].path} == ingress_entries_paths

    # # Check if valid_scans actually corresponds to the scans that are written
    assert {valid_scans[0].path, valid_scans[1].path}.issubset(ingress_entries_paths)
    assert not {valid_scans[0].path, valid_scans[1].path} == ingress_entries_paths
    assert {valid_scans[0].hash, valid_scans[1].hash} == {hashed_scans_fresh[0].hash, hashed_scans_fresh[1].hash}
    for scan in valid_scans:
        assert scan.state.get_state() == 'hashed'    


def test_storage_write(storage_from_storage_fs, ingress_scan_db):
    '''
    Here we are in the situation that files from ingress have alread been moved to storage, thus
    already recorded in the ingress table, but not recorded in the storage table
    '''
    storage_table = StorageTable(ingress_scan_db.sqlite_path)
    storage_scans = storage_from_storage_fs.get_existing_slides(ingress_scan_db.sqlite_path)
    storage_table.write_candidate_scans(storage_scans)
    for scan in storage_scans:
        assert scan.state.get_state() == 'storage_db'
    storage_entries = storage_table.read_all()
    storage_entries_paths = {storage_entry[1] for storage_entry in storage_entries}
    storage_entries_hashes = {storage_entry[0] for storage_entry in storage_entries}
    assert {storage_scans[0].path, storage_scans[1].path} == storage_entries_paths
    assert {storage_scans[0].hash, storage_scans[1].hash} == storage_entries_hashes

def test_storage_write_duplicates(storage_from_storage_fs, storage_scan_db):
    '''
    The storage_scan_db already contains the hashed_scans in its tables,
    so nothing should be added to those tables
    '''
    storage_table = StorageTable(storage_scan_db.sqlite_path)
    candidate_scans = storage_from_storage_fs.get_existing_slides(storage_scan_db.sqlite_path)
    written_scans = storage_table.write_candidate_scans(candidate_scans)
    # Check that nothing is written to the database
    assert not written_scans

    # This is the most crucial test: Integrity of storage and db
    # storage
    path = Path(storage_from_storage_fs.path)
    contents_storage = set(path.rglob("*.ndpi"))
    # storage_db
    storage_entries = storage_table.read_all_of_column('filepath')
    db_storage_paths = {Path(storage_entry) for storage_entry in storage_entries}
    assert db_storage_paths == contents_storage

def test_storage_write_fresh(storage_from_storage_fs_fresh, storage_scan_db, storage_scans_fresh):
    storage_table = StorageTable(storage_scan_db.sqlite_path)
    candidate_scans = storage_from_storage_fs_fresh.get_existing_slides(storage_scan_db.sqlite_path)
    written_scans = storage_table.write_candidate_scans(candidate_scans)
    for scan in written_scans:
        assert scan.state.get_state() == 'storage_db'
    storage_entries = storage_table.read_all()
    storage_entries_paths = {storage_entry[1] for storage_entry in storage_entries}
    storage_entries_hashes = {storage_entry[0] for storage_entry in storage_entries}
    # the not statements check that the already existing entries in the ingress table are not overwritten
    assert {written_scans[0].path, written_scans[1].path}.issubset(storage_entries_paths)
    assert not {written_scans[0].path, written_scans[1].path} == storage_entries_paths
    assert {written_scans[0].hash, written_scans[1].hash}.issubset(storage_entries_hashes)
    assert not {written_scans[0].hash, written_scans[1].hash} == storage_entries_hashes    

    








# def test_database_initialization(filesystem):
#     """Initializes the database to the current storage"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = True)

# def test_ingress_to_storage():
#     """Tests the full machinery for a new ingress folder"""
#     dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = False)
#     dbase.ingress_to_storage(_j(filesystem, "ingress"))
