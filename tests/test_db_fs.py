import os
from os.path import join as _j
from pathlib import Path
from typing import List
import sqlite3

from bellastore.utils.scan import Scan
from bellastore.database.db import Db

def get_files(dir):
    files = Path(dir).rglob("*")
    file_paths = list(files)
    file_paths = {str(file) for file in file_paths if file.is_file()}
    return file_paths

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
def check_ingress_db(db: Db, original_path, scan: Scan):
    # as in ingress we allow for duplicate hashes, issubset is enough
    assert {tuple((scan.hash, original_path , scan.filename))}.issubset(set(get_scan_entry(db.sqlite_path, scan, 'ingress')))

def check_storage_db(db: Db, scans: List[Scan]):
    # in storage we need strict equality
    for scan in scans:
        assert [(scan.hash, scan.path, scan.filename, scan.scanname)] == get_scan_entry(db.sqlite_path, scan, 'storage')


# MAIN TESTS

# Starting from empty database
def test_empty(root_dir, ingress_dir, new_scans):
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    # as the scans will be moved to storage we newwd to record their original paths
    original_paths = []
    for scan in new_scans:
        original_paths.append(scan.path)
    check_ingress(db, new_scans)
    db.insert_many(new_scans)

    # Ingress checks
    for scan, original_path in zip(new_scans, original_paths):
        check_ingress_db(db, original_path, scan)
    check_empty_ingress(db)

    # Storage checks
    check_storage(db, new_scans)
    check_storage_db(db, new_scans)

    print(str(db))   

# Starting from already filled database
def test_classic(root_dir, ingress_dir, new_scans, classic_db):
    # db will be equal to classic_db
    # classic db already holds scans[0:2]=new_scans[0:2] (also in storage)
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    storage_scans = get_scans(_j(root_dir,'storage'))
    # as the scans will be moved to storage we newwd to record their original paths
    original_paths = []
    for scan in new_scans:
        original_paths.append(scan.path)
    check_ingress(db, new_scans)
    db.insert_many(new_scans)

    # Ingress checks
    for scan, original_path in zip(new_scans, original_paths):
        check_ingress_db(db, original_path, scan)
    check_empty_ingress(db)

    # Storage checks
    # as we moved new_scan[2:4] to storage we append them to the storage scans
    storage_scans.extend(new_scans[2:4])
    check_storage(db, storage_scans)
    # print(storage_scans)
    check_storage_db(db, storage_scans)
    check_not_in_storage(db, new_scans[0:2])

    print(str(db))




    




