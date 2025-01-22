import pytest
import sqlite3
import os
from os.path import join as _j

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.filesystem.storage import Storage 

def test_scan_integrity(hashed_scans):
    for scan in hashed_scans:
        assert scan.state.get_state() == 'hashed'

def test_insert_storage(fs, empty_scan_db, hashed_scans):
    storage = Storage(fs.storage_dir)
    # move slides from ingress to storage
    storage.insert_files(hashed_scans)
    # test wether each scan is sucessfully moved to storage
    for scan in hashed_scans:
        assert scan.path == _j(fs.storage_dir, scan.hash, scan.filename)
        assert scan.state.get_state() == 'storage'
    scans_in_storage = storage.get_existing_slides(empty_scan_db.sqlite_path)
    # check equality of filenames
    files_in_storage = {scan.filename for scan in scans_in_storage}
    hashed_files = {scan.filename for scan in hashed_scans}
    assert hashed_files == files_in_storage
    # check equality of hashes
    hashes_in_storage = {scan.hash for scan in scans_in_storage}
    hashes = {scan.hash for scan in hashed_scans}
    assert hashes_in_storage == hashes
    # check equality of states
    states_in_storage = {scan.state.get_state() for scan in scans_in_storage}
    states = {scan.state.get_state() for scan in hashed_scans}
    assert states_in_storage == states
    



