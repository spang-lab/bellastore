import os
from os.path import join as _j
from pathlib import Path
from typing import List
import sqlite3

from bellastore.utils.scan import Scan
from bellastore.database.db import Db
from conftest import get_files

def test_fs(root_dir, ingress_dir):
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    valid_scan_paths = {scan.path for scan in db.get_valid_scans_from_ingress()}
    assert valid_scan_paths == get_files(ingress_dir)

def test_fs_with_subfolders(root_dir, ingress_dir_with_subfolders):
    db = Db(root_dir, ingress_dir_with_subfolders, 'scans.sqlite')
    valid_scan_paths = {scan.path for scan in db.get_valid_scans_from_ingress()}
    assert valid_scan_paths.issubset(get_files(ingress_dir_with_subfolders))

