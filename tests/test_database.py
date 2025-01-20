import pytest
from os.path import join as _j
from bellastore.database.database import ScanDatabase

def test_database_initialization(filesystem):
    """Initializes the database to the current storage"""
    dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = True)

def test_ingress_to_storage():
    """Tests the full machinery for a new ingress folder"""
    dbase = ScanDatabase(path = _j(filesystem, "storage"), update_database = False)
    dbase.ingress_to_storage(_j(filesystem, "ingress"))
