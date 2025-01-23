from typing import List, Dict, Tuple
import os
import glob
import shutil
from pathlib import Path
import sqlite3
from concurrent.futures import ThreadPoolExecutor

from bellastore.utils.scan import Scan
from bellastore.filesystem.base_folder import BaseFolder
from .constants import scan_extensions_glob, scan_extensions

class Storage(BaseFolder):
    def __init__(self, path):
        super().__init__(path)
    
    def insert(self, scan: Scan):
        if scan.hash: # This condition is just for correct typing as theoretically `hash` is also allowed to be `None` (but which it can't be at this point)
            target_folder = os.path.join(self.path, scan.hash)
            os.makedirs(target_folder, exist_ok = False) # If the folder already exists we are in big problems - it should have been filtered out by then...

            # Move the slide to the storage folder (`.mrxs` is handled automatically)
            new_scan_path = self.move_file(scan.path, target_folder)
            # of course we also need to adjust the scan path after moving
            scan.path = new_scan_path
            scan.state.move_forward()
        else:
            raise RuntimeError("Error with state machine, scan is not hashed.")

    def insert_many(self, scans: List[Scan]) -> List[Scan]:
        for scan in scans:
            self.insert(scan)
        return scans
    
    def check_storage_integrity(self, sqlite_path: str, check_sqlite = False, verbose = True) -> bool:
        """
        Checks the integrity of the current storage. This includes:
        - each folder contains exactly one scanner file
        - the scanner file's hash matches the folder name
        - (if `check_sqlite` is selected): if the file's hash is in the storage table
        - (if `check_sqlite` is selected): if every hash from the storage table is actually there

        **CARE**: this calculates the hash of every single file in the storage, this might take some time!

        Args:
            check_sqlite (bool, default = False): checks if the sqlite conditions are also met
            verbose (bool, default = True): prints out anything that prevents the storage from integrity

        Returns:
            has_integrity (bool): bool indicating if the storage's current integrity
        """
        if verbose:
            print("Checking integrity of storage.")
        # -------------------------------------- #
        # --- GATHER THEORETICAL SQLITE DATA --- #
        # -------------------------------------- #

        if check_sqlite:
            # Get existing hashes from the storage table
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            cursor.execute("SELECT hash FROM storage")
            existing_hashes = set([row[0] for row in cursor.fetchall()])
            non_used_hashes = existing_hashes # From here every present hash will be removed - so this should be empty in the end

        # ---------------------------------- #
        # --- GATHER ACTUAL STORAGE DATA --- #
        # ---------------------------------- #

        # This will be set to false if at least one error occurs
        has_integrity : bool = True

        scan_data : List[Tuple[str, str]] = [] # (folder name, fulll path to scanner file)
        for folder_name in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder_name)
            if not os.path.isdir(folder_path):
                continue

            scan_files = []
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    if any(file.endswith(ext) for ext in scan_extensions):
                        scan_files.append(os.path.join(root, file))
                if root.count(os.sep) - folder_path.count(os.sep) >= 1:
                    break

            # Only add valid folders with a single scan file.
            if len(scan_files) == 1:
                scan_data.append((folder_name, scan_files[0]))
            else:
                has_integrity = False
                if verbose:
                    print(f"There are {len(scan_files)} scanner files in folder {folder_name}.")

        # ----------------------------------------------------- #
        # --- USE A ThreadPoolExecutor FOR HASH CALCULATION --- #
        # ----------------------------------------------------- #

        def process_scan(scan_info : Tuple[str, str]) -> Tuple[str, str, str | None]:
            """Processes a scaner file by creating an instance of the `Scan` class and calculating its hash"""
            folder_name, scan_file = scan_info
            scan = Scan(scan_file)
            scan.state.move_forward()
            scan.hash = scan.hash_scan()
            return (folder_name, scan_file, scan.hash)

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(process_scan, scan_data))

        # ----------------------------------------------------- #
        # --- COMPARE THE CALCULATED TO THE EXISTING HASHES --- #
        # ----------------------------------------------------- #
        for folder_name, scan_file, calculated_hash in results:
            # TODO: Lukas check
            if calculated_hash != folder_name:
                print(f"Current folder name {folder_name} is not matching hash {calculated_hash}, so folder will be renamed.")
                os.rename(os.path.join(self.path, folder_name), os.path.join(self.path, calculated_hash))
                # has_integrity = False
                # raise RuntimeError(f"Hash mismatch for file {scan_file} in folder {folder_name}")

            if check_sqlite:
                if calculated_hash not in existing_hashes:
                    has_integrity = False
                    if verbose:
                        print(f"Couldn't find {scan_file}'s hash in the storage table.")
                else:
                    non_used_hashes.remove(calculated_hash)

        if check_sqlite:
            if non_used_hashes:
                has_integrity = False
                if verbose:
                    print(f"There are hashes in the storage table, that are not present in the actual storage:\n{non_used_hashes}")

        return has_integrity
    
    def get_existing_slides(self, sqlite_path: str) -> None:
        """
        Finds already existing and hashed files in the storage folder that are not yet in the sqlite

        It will fail if:
        - there are hashes in the sqlite storage that are not in the storage directory (lost file?!)
        - the function `self.check_storage_integrity(check_sqlite = False, verbose = True)` fails
        - the calculated hash of a scanner file does not match its folder name
        - the folder with the hash to be added has no scanner file at all

        It will not fail if:
        - there are files in the storage that are already in the sqlite and hashed
        - the call `self.check_storage_integrity(check_sqlite = True, verbose = True)` would fail\\
            (it cannot check with the sqlite flag, the whole point of the function is to bring the sqlite up to date)
        """
        if self.check_storage_integrity(sqlite_path=sqlite_path, check_sqlite=False, verbose=True):

            # ------------------------------------------------ #
            # --- COLLECT SQLITE HASHES AND PRESENT HASHES --- #
            # ------------------------------------------------ #
            
            print(f"Obtaining current slide-hashes from 'storage' table")
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            cursor.execute("SELECT hash FROM storage")
            sqlite_hashes = set([row[0] for row in cursor.fetchall()])

            # Get all hashes from existing slides
            print(f"Comparing hashes from 'storage' table to existing ones")
            entries = os.listdir(self.path)
            directory_hashes = set([entry for entry in entries if os.path.isdir(os.path.join(self.path, entry))])

            # Check if there are files that are in the sqlite but not in the directory - this would be a major problem
            non_present_files = sqlite_hashes - directory_hashes
            if non_present_files:
                print(f"There are files in the sqlite table, that are not in the storage!\n"
                    f"I will abort the updating process without any writing.\n"
                    f"The non-present ones are the following:\n{non_present_files}")
                return

            # Find the slides/hashes that are in the directory but not yet in the sqlite
            files_to_add = directory_hashes - sqlite_hashes

            # ------------------------------ #
            # --- FIND THE SCANNER FILES --- #
            # ------------------------------ #

            scan_data = []
            for hash_value in files_to_add:
                folder_path = os.path.join(self.path, hash_value)
                scan_file_path = None

                # Search for the scanner file in the folder
                for root, dirs, files in os.walk(folder_path):
                    for file in files:
                        if any(file.endswith(ext) for ext in scan_extensions):
                            scan_file_path = os.path.join(root, file)
                            break
                    if scan_file_path:
                        break

                # If we find the file, add it to the list for further processing
                if scan_file_path:
                    scan_data.append((hash_value, scan_file_path))
                else:
                    print(f"No valid scan file found in folder {hash_value}. Please fix this first.")
                    return

            # -------------------------------------------------------------------------- #
            # --- CALCULATE WHAT THE HASHES SHOULD BE FOR UNREGISTERED SCANNER FILES --- #
            # -------------------------------------------------------------------------- #

            def process_scan(scan_info):
                hash_value, scan_file = scan_info
                scan = Scan(scan_file)
                scan.state.move_forward()
                scan.hash = scan.hash_scan()
                scan.state.move_forward()
                # now the slide is only hashed, but as it is also in storage we give it
                # the storage state
                scan.state.move_forward()
                return (hash_value, scan)

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(process_scan, scan_data))

            # --------------------------------------------------------------------- #
            # --- COMPARE CALCULATED HASHES TO FOLDER NAMES AND WRITE TO SQLITE --- #
            # --------------------------------------------------------------------- #

            scans = []
            for hash_value, scan in results:
                if scan.hash == hash_value:
                    scans.append(scan)
                else:
                    raise RuntimeError(f"Hash mismatch for file {scan.path} in folder {hash_value}\n"
                                       f"The now calculated hash is {scan.hash}, but should be {hash_value}")
            return scans

        else:
            print(f"\nFailed to update for existing slides. The current storage structure is not correct.")
            return