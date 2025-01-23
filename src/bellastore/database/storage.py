from typing import List, Dict, Tuple
import glob
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from os.path import join as _j

# TODO: where to put such classes ?
from bellastore.utils.scan import Scan
from .base_table import BaseTable
from bellastore.utils.helpers import get_unique_scans

class StorageTable(BaseTable):
    def __init__(self, sqlite_path):
        super().__init__(sqlite_path, 'storage')

    def get_scans_to_insert(self, candidate_scans: List[Scan], verbose: False):

        unique_scans = get_unique_scans(candidate_scans, verbose)
        existing_hashes = set(self.read_all_of_column('hash'))
        # Prepare the list of scans that need to be inserted (filter out existing ones)
        scans_to_insert = [scan for scan in unique_scans if scan.hash not in existing_hashes]
        return scans_to_insert
    
    def write_candidate_scans(self, candidate_scans : List[Scan], verbose : bool = False) -> List[Scan]:
        """
        This **only writes** and **does not move** a list of slides to the storage table.
        To be used with much care as wrong usage will end up
        with slides being in the storage table, but not in the actual storage!

        Args:
            candidate_scans (List[Scan]): list of scans with state "hashed"
                (there still might be non-unique hashes, they get filtered)
            verbose (bool, default = False): print duplicate hashes

        Returns:
            inserted_scans (List[Scan]): list containing only the scans that have been added to the storage table
        """
        scans_to_insert = self.get_scans_to_insert(candidate_scans, verbose)

        # Insert the new scans into the storage table
        self.write_many(scans_to_insert)
        return scans_to_insert
    
    def write(self, scan: Scan):
        # TODO: check with Lukas if this is correct
        # caution the path will be changed after moving so we need to give the final path here
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {self.name} (hash, filepath, filename, scanname) 
            VALUES (?, ?, ?, ?)
        """, (scan.hash, scan.path, scan.filename, scan.scanname))
        # in order to have the state storage_db
        scan.state.move_forward()
        conn.commit()
    
    def write_many(self, scans: List[Scan]):
        # TODO: check with Lukas if this is correct
        # caution the path will be changed after moving so we need to give the final path here
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        for scan in scans:
            cursor.execute(f"""
                INSERT INTO {self.name} (hash, filepath, filename, scanname) 
                VALUES (?, ?, ?, ?)
            """, (scan.hash, scan.path, scan.filename, scan.scanname))
            # in order to have the state storage_db
            scan.state.move_forward()
        conn.commit()



    


# TODO functions for `myc`, `stain`, etc tables
