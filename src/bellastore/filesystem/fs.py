import os
from os.path import join as _j
from pathlib import Path
import pytest
import sqlite3
from typing import List, Dict, Tuple
import functools

from bellastore.utils.scan import Scan

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

    @staticmethod
    def _get_files(dir):
        files = Path(dir).rglob("*")
        file_paths = list(files)
        file_paths = [str(file) for file in file_paths if file.is_file()]
        return file_paths

    
    def _add_scan_to_ingress(self, scan: Scan):
        # Moving to ingress means hashing
        scan.hash_scan()
        scan.move(self.ingress_dir)
    def add_scan_to_storage(self, scan: Scan):
        self.add_scan_to_ingress(scan)
        target_dir = os.path.join(self.storage_dir, scan.hash)
        scan.move(target_dir)
    def _add_scans_to_ingress(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress(scan)
    def add_scans_to_storage(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage(scan)
    
    def get_files_from_ingress(self):
        print(f"The files in {self.ingress_dir} are")
        return self._get_files(self.ingress_dir)
    def get_files_from_storage(self):
        print(f"The files in {self.storage_dir} are")
        return self._get_files(self.storage_dir)
    
    def __str__(self):
        ingress = self.get_files_from_ingress()
        storage = self.get_files_from_storage()
        return(f"Ingress: {ingress}\n Storage: {storage}")