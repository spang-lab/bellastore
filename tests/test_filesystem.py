import pytest
import sqlite3
import os
from os.path import join as _j
from pathlib import Path

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.filesystem.storage import Storage
from bellastore.filesystem.ingress import Ingress

from conftest import Fs


def test_filesystem(test_fs: Fs):
    # files = Path(test_fs.storage_dir).rglob("*")
    # file_paths = list(files)
    # file_paths = [str(file_path) for file_path in file_paths]
    # print(file_paths)
    print(test_fs.get_files_from_ingress())
    print(test_fs.get_files_from_storage())

def test_scan_integrity(hashed_scans):
    for scan in hashed_scans:
        assert scan.state.get_state() == 'hashed'


def test_ingress_get_files(ingress_fs):
    ingress = Ingress(ingress_fs.ingress_dir)
    assert ingress.path == ingress_fs.ingress_dir
    files = set(ingress.get_files())
    assert files == ingress_fs.files


def test_storage_insert_files(ingress_fs, hashed_scans):
    '''
    Independently of any database, this checks wether the inserting of
    scans into the storage works properly
    '''
    storage = Storage(ingress_fs.storage_dir)
    # move slides from ingress to storage
    moved_scans = storage.insert_many(hashed_scans)
    # test wether each scan is sucessfully moved to storage
    for scan in hashed_scans:
        assert scan.path == _j(ingress_fs.storage_dir, scan.hash, scan.filename)
        assert scan.state.get_state() == 'storage'
    for scan in moved_scans:
        assert scan.path == _j(ingress_fs.storage_dir, scan.hash, scan.filename)
        assert scan.state.get_state() == 'storage'

def test_storage_insert_files_duplicate(storage_fs, hashed_scans):
    '''
    Independently of any database, this checks wether the inserting of
    scans into the storage works properly
    '''
    storage = Storage(storage_fs.storage_dir)
    # move slides from ingress to storage
    moved_scans = storage.insert_many(hashed_scans)
    # test wether each scan is sucessfully moved to storage
    print(moved_scans)
    # for scan in hashed_scans:
    #     assert scan.path == _j(ingress_fs.storage_dir, scan.hash, scan.filename)
    #     assert scan.state.get_state() == 'storage'
    # for scan in moved_scans:
    #     assert scan.path == _j(ingress_fs.storage_dir, scan.hash, scan.filename)
    #     assert scan.state.get_state() == 'storage'



def test_storage_check_storage_integrity(ingress_fs, empty_scan_db):
    storage = Storage(ingress_fs.storage_dir)
    assert storage.check_storage_integrity(empty_scan_db.sqlite_path, check_sqlite = True)

def test_storage_get_existing_slides(storage_fs, empty_scan_db, storage_scans):
    '''
    Checks wether the feature of comparing slides in storage to slides in the storage table
    yields the correct scans
    '''
    storage = Storage(storage_fs.storage_dir)
    scans_in_storage = storage.get_existing_slides(empty_scan_db.sqlite_path)
    # check equality of filenames
    files_in_storage = {scan.filename for scan in scans_in_storage}
    hashed_files = {scan.filename for scan in storage_scans}
    assert hashed_files == files_in_storage
    # check equality of hashes
    hashes_in_storage = {scan.hash for scan in scans_in_storage}
    hashes = {scan.hash for scan in storage_scans}
    assert hashes_in_storage == hashes
    # check equality of states
    states_in_storage = {scan.state.get_state() for scan in scans_in_storage}
    states = {scan.state.get_state() for scan in storage_scans}
    assert states_in_storage == states

    



