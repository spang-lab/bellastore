# This test module holds the integration tests

import os
from os.path import join as _j
from pathlib import Path
from typing import List
import sqlite3

from bellastore.utils.scan import Scan
from bellastore.database.db import Db
from conftest import get_files

def get_files_dirs(dir):
    files = Path(dir).rglob("*")
    file_paths = list(files)
    file_paths = {str(file) for file in file_paths}
    return file_paths

def get_scans(dir):
    scans = []
    files = get_files(dir)
    for file in files:
        scan = Scan(file)
        if not scan.is_valid():
            continue
        scan.hash_scan()
        print(scan.hash)
        scans.append(scan)
    return scans


def get_scan_entry(sqlite_path, scan: Scan, table_name: str):
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE hash = '{scan.hash}'")
        data = cursor.fetchall()
    return data
    

# Fs checks
def check_ingress(db: Db, scans: List[Scan]):
    files_in_ingress = get_files(db.ingress_dir)
    for scan in scans:
        assert {scan.path}.issubset(files_in_ingress)

def check_storage(db: Db, scans: List[Scan]):
    files_in_storage = get_files(db.storage_dir)
    print(files_in_storage)
    for scan in scans:
        assert {scan.path}.issubset(files_in_storage)
    assert {db.sqlite_path}.issubset(files_in_storage)

def check_not_in_storage(db: Db, scans: List[Scan]):
    files_in_storage = get_files(db.storage_dir)
    for scan in scans:
        assert not {scan.path}.issubset(files_in_storage)

def check_empty_ingress(db: Db):
    assert get_files_dirs(db.ingress_dir) == set()

# Db checks
def check_ingress_db(db: Db, scan: Scan):
    # as in ingress we allow for duplicate hashes, issubset is enough
    assert {tuple((scan.hash, _j(db.ingress_dir, scan.filename) , scan.filename))}.issubset(set(get_scan_entry(db.sqlite_path, scan, 'ingress')))

def check_ingress_db_subfolder(db: Db, scan: Scan):
    # as in ingress we allow for duplicate hashes, issubset is enough
    assert {tuple((scan.hash, _j(db.ingress_dir, scan.scanname, scan.filename) , scan.filename))}.issubset(set(get_scan_entry(db.sqlite_path, scan, 'ingress')))

def check_storage_db(db: Db, scans: List[Scan]):
    # in storage we need strict equality
    for scan in scans:
        assert [(scan.hash, scan.path, scan.filename, scan.scanname)] == get_scan_entry(db.sqlite_path, scan, 'storage')



# MAIN TESTS

# Starting from empty database
def test_empty(root_dir, ingress_dir):
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    # as the scans will be moved to storage we need to record their original paths
    check_ingress(db, get_scans(db.ingress_dir))
    scans = db.insert_from_ingress()

    # Ingress checks
    for scan in scans:
        check_ingress_db(db, scan)
    check_empty_ingress(db)

    # Storage checks
    check_storage(db, scans)
    check_storage_db(db, scans)

    print(str(db))   

# Starting from already filled database
def test_classic(root_dir, ingress_dir, classic_db):
    # db will be equal to classic_db
    # classic db already holds 2 scans (also in storage)
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    storage_scans = get_scans(_j(root_dir,'storage'))
    check_ingress(db, get_scans(db.ingress_dir))
    scans = db.insert_from_ingress()

    # Ingress checks
    for scan in scans:
        check_ingress_db(db, scan)
    check_empty_ingress(db)

    # Storage checks
    for scan in scans:
        if scan.path:
            storage_scans.append(scan)
        else:
            check_not_in_storage(db, [scan])

    check_storage(db, storage_scans)
    check_storage_db(db, storage_scans)

    print(str(db))

def test_empty_from_subfolder(root_dir, ingress_dir_with_subfolders):
    db = Db(root_dir, ingress_dir_with_subfolders, 'scans.sqlite')
    # as the scans will be moved to storage we need to record their original paths
    check_ingress(db, get_scans(db.ingress_dir))
    scans = db.insert_from_ingress()

    # Ingress checks
    for scan in scans:
        check_ingress_db_subfolder(db, scan)
        {_j(db.ingress_dir, scan.scanname, f"slide_{scan.filename.split('_')[1]}.sqlite")}.issubset(get_files_dirs(db.ingress_dir))

    # Storage checks
    check_storage(db, scans)
    check_storage_db(db, scans)

    print(str(db)) 

# Starting from already filled database
def test_classic_subfolder(root_dir, ingress_dir_with_subfolders, classic_db_subfolders):
    # db will be equal to classic_db_subfolders
    # classic db already holds 2 scans (also in storage)
    db = Db(root_dir, ingress_dir_with_subfolders, 'scans.sqlite')
    storage_scans = get_scans(_j(root_dir,'storage'))
    check_ingress(db, get_scans(db.ingress_dir))
    scans = db.insert_from_ingress()

    # Ingress checks
    for scan in scans:
        check_ingress_db_subfolder(db, scan)
        {_j(db.ingress_dir, scan.scanname, f"slide_{scan.filename.split('_')[1]}.sqlite")}.issubset(get_files_dirs(db.ingress_dir))

    # Storage checks
    for scan in scans:
        if scan.path:
            storage_scans.append(scan)
        else:
            check_not_in_storage(db, [scan])

    check_storage(db, storage_scans)
    check_storage_db(db, storage_scans)

    print(str(db))




    




