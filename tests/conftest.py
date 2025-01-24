import os
from os.path import join as _j
from pathlib import Path
import pytest
import sqlite3
from typing import List
import functools
import pandas as pd

from bellastore.utils.scan import Scan
    

# CREATING SCANS TO BE PUT INTO OUR FILESYSTE
def create_scans(path: Path, amount = 4) -> List[Scan]:
    scans = []
    for i in range(amount):
        p = path / f"scan_{i}.ndpi"
        p.write_text(f"Content of scan_{i}.ndpi", encoding="utf-8")
        scan = Scan(str(p))
        scans.append(scan)
    return scans

# FILESYSTEMS
# The idea here is to set up filesystems with different layout

# everything starts at a temporary root
@pytest.fixture(scope="function")
def root_dir(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    yield root_dir

@pytest.fixture(scope="function")
def scans(root_dir):
    tmp_path = root_dir / "old_scans"
    os.mkdir(tmp_path)
    yield create_scans(tmp_path, 4)

@pytest.fixture(scope="function")
def new_scans(root_dir):
    tmp_path = root_dir / "new_scans"
    os.mkdir(tmp_path)
    yield create_scans(tmp_path, 4)

@pytest.fixture(scope="function")
def ingress_dir(new_scans):
    '''
    The ingress dir is where the new scans are stored
    '''
    return os.path.dirname(new_scans[0].path)

# blueprint fs
class Fs:
    def __init__(self, root_dir):
        '''
        A Fs only contains a storage and an ingress (and of course a root).
        So this serves as a blueprint for all subsequent filesystems
        '''
        self.root_dir = root_dir
        self.storage_dir = _j(root_dir, "storage")
        os.makedirs(self.storage_dir, exist_ok = True)
        self.ingress_dir = _j(root_dir, "ingress")
        os.makedirs(self.ingress_dir, exist_ok = True)

    
    def add_scan_to_ingress(self, scan: Scan):
        # Moving to ingress means hashing
        scan.hash_scan()
        # scan.move(self.ingress_dir)
    def add_scan_to_storage(self, scan: Scan):
        self.add_scan_to_ingress(scan)
        target_dir = os.path.join(self.storage_dir, scan.hash)
        scan.move(target_dir)
    def add_scans_to_ingress(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress(scan)
    def add_scans_to_storage(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage(scan)

@pytest.fixture(scope="function")
def classic_fs(root_dir, ingress_dir, new_scans):
    fs = Fs(root_dir, ingress_dir)
    fs.add_scans_to_ingress()



# DATABSES
def sqlite_connection(func):
    '''
    This decorator is genious
    '''
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            try:
                result = func(self, cursor, *args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise e
    return wrapper

class Db(Fs):
    def __init__(self, root_dir, filename):
        super().__init__(root_dir)
        self.filename = filename
        self.sqlite_path = os.path.join(self.storage_dir, self.filename)
        self._initialize_db()

    @sqlite_connection
    def _initialize_db(self, cursor):
        cursor.execute('''
        CREATE TABLE ingress (
            hash TEXT,
            filepath TEXT,
            filename TEXT,
            UNIQUE(hash, filepath, filename)
        )
        ''')
        cursor.execute('''
        CREATE TABLE storage (
            hash TEXT NOT NULL PRIMARY KEY,
            filepath TEXT,
            filename TEXT,
            scanname TEXT,
            FOREIGN KEY(hash) REFERENCES ingress(hash)
        )
        ''')

    @sqlite_connection  
    def add_scan_to_ingress_db(self, cursor, scan: Scan):
        self.add_scan_to_ingress(scan)
        cursor.execute(
            f"INSERT INTO ingress (hash, filepath, filename) VALUES (?, ?, ?)",
            (scan.hash, scan.path, scan.filename)
        )
    def add_scans_to_ingress_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress_db(scan)

    @sqlite_connection  
    def add_scan_to_storage_db(self, cursor, scan: List[Scan]):
        self.add_scan_to_ingress_db(scan)
        # This is super important in order to have hashing consistent
        self.add_scan_to_storage(scan)
        cursor.execute(f"""
            INSERT INTO storage (hash, filepath, filename, scanname) 
            VALUES (?, ?, ?, ?)
            """, (scan.hash, scan.path, scan.filename, scan.scanname))

    def add_scans_to_storage_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage_db(scan)

@pytest.fixture(scope="function")
def empty_db(root_dir):
    db = Db(root_dir, 'scans.sqlite')
    return db

@pytest.fixture(scope="function")
def ingress_only_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    db.add_scans_to_ingress_db(scans)
    return db

@pytest.fixture(scope="function")
def storage_only_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    db.add_scans_to_storage_db(scans)
    return db

@pytest.fixture(scope="function")
def classic_db(root_dir, scans):
    db = Db(root_dir, 'scans.sqlite')
    # db.add_scans_to_ingress_db(scans[0:2])
    db.add_scans_to_storage_db(scans[0:2])
    return db