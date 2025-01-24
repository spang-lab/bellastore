import os
from os.path import join as _j
from pathlib import Path
from typing import List

from bellastore.utils.scan import Scan

# blueprint fs
class Fs:
    def __init__(self, root_dir, ingress_dir):
        '''
        A Fs only contains a storage and an ingress (and of course a root).
        So this serves as a blueprint for all subsequent filesystems
        '''
        self.root_dir = root_dir
        self.ingress_dir = ingress_dir
        self.storage_dir = _j(root_dir, "storage")
        os.makedirs(self.storage_dir, exist_ok = True)
        self.ingress_dir = ingress_dir
        os.makedirs(self.ingress_dir, exist_ok = True)

    @staticmethod
    def _get_files(dir):
        files = Path(dir).rglob("*")
        file_paths = list(files)
        file_paths = [str(file) for file in file_paths if file.is_file()]
        return file_paths
    
    def get_valid_scans_ingress(self) -> List[Scan]:
        scans = []
        files = self._get_files(self.ingress_dir)
        for file in files:
            scan = Scan(file)
            if not scan.is_valid():
                continue
            scan.hash_scan()
            print(scan.hash)
            scans.append(scan)
        return scans

    
    def _add_scan_to_ingress(self, scan: Scan):
        # Moving to ingress is equivalent to hashing
        scan.hash_scan()
    def add_scan_to_storage(self, scan: Scan):
        # self._add_scan_to_ingress(scan)
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
    


    def print_tree(self, path=None, prefix=''):
        if path is None:
            path = Path(self.root_dir)
        else:
            path = Path(path)
        
        contents = sorted(path.iterdir(), key=lambda p: p.name)
        
        for i, item in enumerate(contents):
            connector = '└── ' if i == len(contents) - 1 else '├── '
            print(f'{prefix}{connector}{item.name}')
            
            if item.is_dir():
                extension = '    ' if i == len(contents) - 1 else '│   '
                self.print_tree(path=item, prefix=prefix+extension)