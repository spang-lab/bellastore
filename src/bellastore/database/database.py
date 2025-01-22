# This module acts on the database as a whole, 
# i.e. here we can implement methods like creating,
# deleting and checking the integrity of the database

import sqlite3
import os

from .tables import check_tables
from .constants import scan_extensions_glob, scan_extensions
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor



class ScanDatabase():
    """
    The class to manage the storage's database.
    You may also create an instance of this at a position where storage and database already exist, it will use the present sqlite table.

    Init
    ----
        **path** _(str)_ : path to the folder, where the storage should be created into
        **update_database** _(bool)_ : indicating if the database should be updated during initialization

    Attributes:
        path (str): path to the folder, where the storage should be created into
        sqlite_path (str): full path to the sqlite database

    Methods
    -------
    <p>
        **ingress_to_storage**<em>(self, dirpath : str, verbose = False) -> List[Scan]</em> <br> adds a new directory to the storage (basically does the full machinery)<br>
        **create_database**<em>(self) -> None</em> <br> creates the initial sqlite database<br>
        **update_for_existing_slides**<em>(self) -> None</em> <br> adds already hashed slides from the storage to the database (good for already existing storages with a new sqlite)<br>
        **get_scans_from_ingress**<em>(self, dirpath : str, verbose = False) -> List[str]</em> <br> creates a list of scanner files from the ingress, excluding the storage<br>
        **write_to_ingress**<em>(self, considered_scans: List[str], verbose: bool = False) -> List[Scan]</em> <br> writes a list of scanner paths to the ingress, returns the written scans<br>
        **move_to_storage**<em>(self, scans_to_move: List[Scan]) -> List[Scan]</em> <br> moves a list of scans to the storage, returns the written scans<br>
        **check_storage_integrity**<em>(self, check_sqlite = False, verbose = True) -> bool</em> <br> checks if everything is all right with the storage
    </p>
    """
    def __init__(self, path : str):
        self.path = path
        self.writeable = False
        self.sqlite_path = None
        # This really is the intended behaviour, for creating the storage you should really avoid any typos
        if not os.path.exists(self.path):
            raise ValueError("Given path does not exist. Make sure it exists first or that there are no typos!")
        
        sqlite_path = os.path.join(path, "scans.sqlite")
        try:
            check_tables(sqlite_path)
            self.sqlite_path = os.path.join(path, "scans.sqlite")
            self.writeable = True
        except Exception as e:
            raise RuntimeError(e)

        # # Update sqlite for possibly existing slides, then check the database integrity
        # if update_database:
        #     self.update_for_existing_slides()
        #     if not self.check_storage_integrity(check_sqlite = True, verbose = True):
        #         raise ValueError(f"Could not match current storage to sqlite.")

    
    def open(self):
        pass

    def open_read_write(self):
        pass

    def open_readonly(self):
        pass

    def is_writeable(self):
        return self.writeable

    def check_writeable(self):
        if not self.writeable:
            raise RuntimeError("Database is not writeable")