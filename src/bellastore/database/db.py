import os
from os.path import join as _j
import sqlite3
from typing import List
import functools
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

from bellastore.filesystem.fs import Fs
from bellastore.utils.scan import Scan

# DATABSES
def sqlite_connection(func):
    ''' 
    Decorator allowing for comprehensive sqlite connection
    '''
    
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            try:
                result = func(self, cursor, *args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise e
    return wrapper

class Db(Fs):
    ''' 
    A class representing an sqlite databse abstracting the underlying file system

    Attributes
    ----------
    filename: str
        The name of the database, classically `scans.sqlite`
    sqlite_path:
        The path to the database.
        The databse is placed always on top level within the storage directory.
    
    Methods
    -------
    insert_from_ingress:
        The method for inserting several scans to the storage
    '''

    def __init__(self, root_dir, ingress_dir, filename):
        super().__init__(root_dir, ingress_dir)
        self.filename = filename
        self.sqlite_path = os.path.join(self.storage_dir, self.filename)
        self._initialize_db()

    @sqlite_connection
    def _table_exists(self, cursor, table_name: str):
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table_name, ))
        result = cursor.fetchall()
        if result:
            return True
        else:
            False

    @sqlite_connection
    def _initialize_db(self, cursor):
        if not self._table_exists('ingress'):
            cursor.execute('''
            CREATE TABLE ingress (
                hash TEXT,
                filepath TEXT,
                filename TEXT,
                UNIQUE(hash, filepath, filename)
            )
            ''')
        if not self._table_exists('storage'):
            cursor.execute('''
            CREATE TABLE storage (
                hash TEXT NOT NULL PRIMARY KEY,
                filepath TEXT,
                filename TEXT,
                scanname TEXT,
                FOREIGN KEY(hash) REFERENCES ingress(hash)
            )
            ''')

    @sqlite_connection  
    def add_scan_to_ingress_db(self, cursor, scan: Scan, rec = False):
        # rec tells us if the scan is already recorded in storage, so we do not
        # have to physically move the file around
        if not rec:
            self._add_scan_to_ingress(scan)
        print(f"Recording scan in ingress")
        cursor.execute(
            f"INSERT INTO ingress (hash, filepath, filename) VALUES (?, ?, ?)",
            (scan.hash, scan.path, scan.filename)
        )
    def add_scans_to_ingress_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_ingress_db(scan)

    @sqlite_connection  
    def add_scan_to_storage_db(self, cursor, scan: List[Scan]):
        self.add_scan_to_ingress_db(scan)
        # This is super important
        self.add_scan_to_storage(scan)
        print(f"Recording scan in storage")
        cursor.execute(f"""
            INSERT INTO storage (hash, filepath, filename, scanname) 
            VALUES (?, ?, ?, ?)
            """, (scan.hash, scan.path, scan.filename, scan.scanname))

    def add_scans_to_storage_db(self, scans: List[Scan]):
        for scan in scans:
            self.add_scan_to_storage_db(scan)

    def insert(self, scan: Scan):
        ''' 
        Inserts a single scan into the storage database.

        Inserting into the storage follows multiple steps:
        - check wether scan is already recorded in ingress
        - record scan in storage db (if not existent)
        - move scan file to storage (if not existent)
        '''
        scan.hash_scan()
        print(f"\nStarting insert pipeline for {scan.hash}:")
        existing_ingress_entries = self.get_entries_from_ingress_db()
        if (scan.hash, scan.path, scan.scanname) in existing_ingress_entries:
            print(f'Scan already recorded in the ingress table and thus scan will be deleted.')
            os.remove(scan.path)
            scan.path = None
            return
        existing_storage_entries = self.get_entries_from_storage_db()
        existing_storage_hashes = [entry[0] for entry in existing_storage_entries]
        if scan.hash in existing_storage_hashes:
            print(f'Scan already recorded in the storage table, so scan will be only recorded in ingress table and then deleted.')
            self.add_scan_to_ingress_db(scan, rec = True)
            print(f'Deleting scan')
            os.remove(scan.path)
            scan.path = None
            return
        # In this case the scan is not recorded, thus also not in storage so we run the whole pipeline
        # This will also automatically care about the storage backend recursively
        # TODO: the scan is already hashed at this place, so we do not need to hash it again
        self.add_scan_to_storage_db(scan)
    
    def insert_many(self, scans: List[Scan]):
        for scan in scans:
            self.insert(scan)
    
    def insert_from_ingress(self):
        ''' This is the main insert function

        This function retrieves valid scans from the ingress, inserts
        those into the storage and removes the resulting empty folders from the ingress.

        Return
        ------
            Valid scans (no matter if already in storage or not)
        '''
        scans = self.get_valid_scans_from_ingress()
        self.insert_many(scans)
        self.remove_empty_folders()
        return scans



    @sqlite_connection 
    def _read_all(self, cursor, table_name: str):
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        return data
      
    def _read_all_pd(self, table_name: str):
        conn = sqlite3.connect(self.sqlite_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df


    def get_entries_from_ingress_db(self):
        return self._read_all('ingress')
    
    def get_entries_from_storage_db(self):
        return self._read_all('storage')
    
    def setup_logging(self):
        """Configure logging to both file and console"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(_j(self.backup_dir, 'database_backup.log')),
                logging.StreamHandler()
            ]
        )
    

    def create_backup(self, max_backups=10):
        """
        Create a backup of the SQLite database
        
        Args:
            max_backups (int): Maximum number of backup files to keep
        """
        try:
            
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            db_name = Path(self.sqlite_path).stem
            backup_path = Path(self.backup_dir) / f"{db_name}_backup_{timestamp}.sqlite"
            
            # Ensure source database exists
            if not Path(self.sqlite_path).exists():
                raise FileNotFoundError(f"Database file not found: {self.sqlite_path}")
            
            # Create backup using SQLite's backup API
            with sqlite3.connect(self.sqlite_path) as source:
                with sqlite3.connect(str(backup_path)) as target:
                    source.backup(target)
            
            logging.info(f"Backup created successfully: {backup_path}")
            
            # Clean up old backups if exceeding max_backups
            self._cleanup_old_backups(max_backups)
            
            return True
            
        except Exception as e:
            logging.error(f"Backup failed: {str(e)}")
            return False

    def _cleanup_old_backups(self, max_backups):
        """Remove oldest backups if exceeding max_backups limit"""
        backups = sorted(
            Path(self.backup_dir).glob('*_backup_*.sqlite'),
            key=os.path.getctime
        )
        
        while len(backups) > max_backups:
            oldest_backup = backups.pop(0)
            oldest_backup.unlink()
            logging.info(f"Removed old backup: {oldest_backup}")    

    
    def __str__(self):
        # TODO: print_tree method should return a string to be passed to the return
        print("\n")
        self.print_tree()
        ingress_df = self._read_all_pd('ingress')
        storage_df = self._read_all_pd('storage')
        return(f"Ingress DB:\n {ingress_df.to_string()}\n Storage DB:\n {storage_df.to_string()}\n")