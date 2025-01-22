import pytest
import sqlite3
import os
from os.path import join as _j

from conftest import execute_sql, get_tables, get_scheme
from bellastore.database.database import ScanDatabase
from bellastore.filesystem.storage import Storage 

def test_storage(fs, hashed_scans):
    storage = Storage(fs.storage_dir)
    storage.insert_files(hashed_scans)
    for scan in hashed_scans:
        assert scan.path == _j(fs.storage_dir, scan.hash, scan.)

