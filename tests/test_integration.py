import os
from os.path import join as _j
from pathlib import Path

from bellastore.utils.scan import Scan
from bellastore.database.database import ScanDatabase
from bellastore.database.ingress import IngressTable
from bellastore.database.storage import StorageTable
from bellastore.filesystem.ingress import Ingress
from bellastore.filesystem.storage import Storage

# This is the main integration test
def blueprint_integration(fs):
    # Step 1: Initialize empty db
    scan_db = ScanDatabase(fs.root_dir)

    # Step 2: Get files from ingress
    ingress = Ingress(fs.ingress_dir)
    ingress_files = ingress.get_files()

    # Step 3: Record ingress files in ingress table
    ingress_table = IngressTable(scan_db.sqlite_path)
    ingress_scans = ingress_table.write(ingress_files)

    # Step 4: Move ingress files to storage
    storage = Storage(fs.storage_dir)
    moved_scans = storage.insert_files(ingress_scans)

    # Step 5: Get the storage scans
    storage_scans = storage.get_existing_slides(scan_db.sqlite_path)

    # Step 6: Write the storage scans to the storage table
    storage_table = StorageTable(scan_db.sqlite_path)
    storage_table.write(fs.storage_dir, storage_scans)

    # ACTUAL TESTING

    # filesystem
    assert os.listdir(fs.ingress_dir) == []
    assert len(os.listdir(fs.storage_dir)) == 2
    for dir in os.listdir(fs.storage_dir):
        assert len(os.listdir(_j(fs.storage_dir,dir))) == 1

    # databases
    ingress_entries = ingress_table.read_all()
    storage_entries = storage_table.read_all()

    ingress_hashes = {ingress_entry[0] for ingress_entry in ingress_entries}
    storage_hashes = {storage_entry[0] for storage_entry in storage_entries}
    assert ingress_hashes.issubset(storage_hashes)

    ingress_paths = {Path(ingress_entry[1]) for ingress_entry in ingress_entries}
    for p in ingress_paths:
        assert p.parents[1] == Path(fs.ingress_dir)
    
    # filesystem + database integrity
    # This is the most important test: here we check that those paths that are recorded in the db correspond
    # to those in the actual filesystem
    path = Path(fs.storage_dir)
    contents_storage = set(path.rglob("*.ndpi"))
    db_storage_paths = {Path(storage_entry[1]) for storage_entry in storage_entries}
    assert db_storage_paths == contents_storage

# The most classical case
def test_ingress_integration(ingress_fs):
    blueprint_integration(ingress_fs)

# Weird case where we have non hashed slides in storage
def test_ingress_integration(storage_fs):
    blueprint_integration(storage_fs)

# TODO: Combination of both

# TODO: running pipeline twice with new ingress in second round




