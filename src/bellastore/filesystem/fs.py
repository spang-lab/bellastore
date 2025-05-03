import os
from os.path import join as _j
from pathlib import Path
from typing import List

from bellastore.utils.scan import Scan

# blueprint fs
class Fs:
    ''' 
    A class representing a simple file system holding:
    - a storage directory
    - an ingress directory
    - a backup directory (for backing up the database)

    Attributes
    ----------
    root_dir: str
        The root of the fs
    ingress_dir: str | None
        The directory holding new candidate scans to be moved to storage.
        If none there we use the class just for inspecting the current state of the fs
    storage_dir: str
        The directory holding all already recorded scans
    backup_dir: str
        The directory holding database backups
    
    Methods
    -------
    _add_scan_to_ingress:
        Method that adds a scan to the ingress. In the sense of the fs the scan is already
        within the ingress so adding means hashing.
    add_scan_to_storage:
        Method to move a scan from the ingress to storage within the file system.
        The recording in the databse will be handled by the Db class
    get_valid_scans_from_ingress:
        Method that scans the ingress directory for valid scans
    remove_empty_folders:
        Method to remove empty folders, resulting from moving scans to storage
    '''

    def __init__(self, root_dir, ingress_dir: None|str):
        self.root_dir = root_dir
        self.ingress_dir = ingress_dir
        self.storage_dir = _j(root_dir, "storage")
        os.makedirs(self.storage_dir, exist_ok = True)
        # we also provide the option just to look at our fs/db without inserting from an ingress
        self.ingress_dir = ingress_dir
        if self.ingress_dir:
            os.makedirs(self.ingress_dir, exist_ok = True)
        self.backup_dir = _j(root_dir, "backup")
        os.makedirs(self.backup_dir, exist_ok=True)

    @staticmethod
    def _get_files(dir):
        files = Path(dir).rglob("*")
        file_paths = list(files)
        file_paths = [str(file) for file in file_paths if file.is_file()]
        return file_paths
    
    def get_valid_scans_from_ingress(self) -> List[Scan]:
        '''
        Method that scans the ingress directory for valid scans
        '''

        scans = []
        print(f'Reading files from ingress directory {self.ingress_dir}')
        files = self._get_files(self.ingress_dir)
        for file in files:
            scan = Scan(file)
            if not scan.is_valid():
                # print(f'Non-valid {scan.path}')
                continue
            print(f'Valid scan: {scan.path}')
            scans.append(scan)
        return scans

    
    def _add_scan_to_ingress(self, scan: Scan):
        '''
        Method that adds a scan to the ingress, i.e. hashes the scan.
        '''

        # Moving to ingress is equivalent to hashing
        scan.hash_scan()
    def add_scan_to_storage(self, scan: Scan):
        '''
        Main function moving scans from ingress to storage
        '''
        
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
    
    def remove_empty_folders(self):
        '''
        Recursively remove empty folders, resulting from moving scans to storage.
        '''
        # Walk through directory tree in bottom-up order
        for root, dirs, files in os.walk(self.root_dir, topdown=False):
            for dir_name in dirs:
                full_path = os.path.join(root, dir_name)
                if full_path == self.backup_dir:
                    continue
                try:
                    # If directory is empty, remove it
                    if not os.listdir(full_path):
                        os.rmdir(full_path)
                        print(f"Removed empty folder: {full_path}")
                except OSError as e:
                    print(f"Error removing {full_path}: {e}")

    


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