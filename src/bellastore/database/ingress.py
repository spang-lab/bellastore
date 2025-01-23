from typing import List, Dict, Tuple
import glob
import sqlite3
from concurrent.futures import ThreadPoolExecutor

from bellastore.utils.scan import Scan
from .base_table import BaseTable

class IngressTable(BaseTable):
    def __init__(self, sqlite_path):
        super().__init__(sqlite_path, 'ingress')

    def write(self, scan: Scan):
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {self.name} (hash, filepath, filename) VALUES (?, ?, ?)",
                (scan.hash, scan.path, scan.scanname)
            )
        except sqlite3.IntegrityError as e:
                print(f"Failed to insert scan {scan.path}: {e}")
        conn.commit()
        conn.close()
        # TODO: here no update of state?
    
    def write_many(self, scans: List[Scan]):
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        for scan in scans:
            try:
                cursor.execute(
                    f"INSERT INTO {self.name} (hash, filepath, filename) VALUES (?, ?, ?)",
                    (scan.hash, scan.path, scan.scanname)
                )
            except sqlite3.IntegrityError as e:
                    print(f"Failed to insert scan {scan.path}: {e}")
        conn.commit()       
        conn.close() 
    

    def write_candidate_scans(self, considered_scans: List[str], verbose: bool = False) -> List[Scan]:
            """
            Writes a list of paths of scanner files and their hashes to the ingress table.
            Non-valid and already existing scans (with the same name and ingress path) will be removed from the list.

            Args:
                considered_scans (List[str]): list of paths to the considered files (call `get_scans_from_ingress` for that)
                verbose (bool, default = False): if `True` this will print out any non-valid file and failed insertion

            Returns:
                valid_scans (List[Scan]): the valid scans on which `INSERT` was called onto, the scans' status is also updated - 
                    note that there still might be non-unique hashes in it (coming from different ingress paths though)
            """
            scans = [Scan(path) for path in considered_scans]
            print(f"Creating {len(scans)} hashes. This might take some time.")

            # Filter only valid scans and hash them
            valid_scans : List[Scan] = []

            def process_scan(scan : Scan) -> None:
                """
                Checks if a scan is valid, moves its state to valid, and hashes it.
                """
                if scan.is_valid():
                    scan.state.move_forward()
                    scan.hash = scan.hash_scan()
                    if scan.hash:
                        valid_scans.append(scan)
                    else:
                        if verbose:
                            print(f"Got empty hash from file {scan.path}")
                elif verbose:
                    # TODO: scan is not actually removed
                    print(f"Invalid scan removed: {scan.path}")

            with ThreadPoolExecutor() as executor:
                executor.map(process_scan, scans)

            # Open the database connection and call `INSERT OR IGNORE`
            print(f"Writing hashes to sqlite.")

            # First get entries from ingress and check against to be inserted to avoid duplicates
            # Select existing entries based on hash, filepath, and filename
            existing_entries = set(self.read_all_of_columns(['hash', 'filepath', 'filename']))

            # Filter out scans that already exist in the ingress table
            # They really do match even though one is received from the sqlite and the other is
            # url safe base64 encoded, as the other originally was too
            valid_scans = [
                scan for scan in valid_scans
                if (scan.hash, scan.path, scan.scanname) not in existing_entries
            ]

            # Write the non-duplicates to the ingress table
            self.write_many(valid_scans)


            # Update the scans, that have been noted in the ingress table, to have the state "hashed"
            for scan in valid_scans:
                scan.state.move_forward()

            # Really only return the scans that have been hashed AND not been in the ingress already
            return valid_scans