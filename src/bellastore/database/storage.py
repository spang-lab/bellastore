from typing import List, Dict, Tuple
import glob
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from os.path import join as _j

# TODO: where to put such classes ?
from bellastore.utils.scan import Scan
from .base_table import BaseTable

class StorageTable(BaseTable):
    def __init__(self):
        super().__init__()
    
    @classmethod
    def write(self, sqlite_path: str, storage_path: str, scans_to_write : List[Scan], verbose : bool = False) -> List[Scan]:
        """
        This **only writes** and **does not move** a list of slides to the storage table.
        To be used with much care as wrong usage will end up
        with slides being in the storage table, but not in the actual storage!

        Args:
            scans_to_write (List[Scan]): list of scans with state "hashed"
                (there still might be non-unique hashes, they get filtered)
            verbose (bool, default = False): print duplicate hashes

        Returns:
            inserted_scans (List[Scan]): list containing only the scans that have been added to the storage table
        """
        # Check if all scans are at least "hashed"
        for i, scan in enumerate(scans_to_write):
            if not scan.state.has_state("hashed"):
                raise ValueError(f"Got a scan at position {i} which is not yet 'hashed'.")

        # Filter for duplicate hashes in input - only insert in the dict, if the hash is not already there
        unique_scans_dict : Dict[str, Scan] = {}

        for scan in scans_to_write:
            if scan.hash:
                if scan.hash not in unique_scans_dict:
                    unique_scans_dict[scan.hash] = scan
                else:
                    if verbose:
                        print(f"Duplicate hash: {scan.hash}")

        unique_scans = list(unique_scans_dict.values())

        # Get existing hashes from the storage table
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute("SELECT hash FROM storage")
        existing_hashes = set([row[0] for row in cursor.fetchall()])

        # Prepare the list of scans that need to be inserted (filter out existing ones)
        scans_to_insert = [scan for scan in unique_scans if scan.hash not in existing_hashes]

        # Insert the new scans into the storage table
        for scan in scans_to_insert:
            # as these slides will be moved 
            # TODO: check with Lukas if this is correct
            # caution the path will be changed after moving so we need to give the final path here
            cursor.execute("""
                INSERT INTO storage (hash, filepath, filename) 
                VALUES (?, ?, ?)
            """, (scan.hash, _j(storage_path, scan.hash), scan.filename))

        conn.commit()

        return scans_to_insert
    


# TODO functions for `myc`, `stain`, etc tables
