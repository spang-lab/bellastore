# This module is designed to check the integrity of the tables and to create tables,
# especially here we define the scheme of our database

import os
import glob
import shutil
import sqlite3

from bellastore.utils.scan import Scan
from .constants import scan_extensions_glob, scan_extensions, tables
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor


def table_exists(name, sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE name=?", (name, ))
    result = cursor.fetchall()
    if result:
        return True
    else:
        False
    

def check_tables(sqlite_path):
    # TODO: check writeable
    for table_name in tables:
        if not table_exists(table_name, sqlite_path):
            print(table_name)
            match table_name:
                case "ingress":
                    create_ingress_table(sqlite_path)
                case "storage":
                    create_storage_table(sqlite_path)
                case "myc":
                    create_myc_table(sqlite_path)
                case "diagnosis":
                    create_diagnosis_table(sqlite_path)
                case "survival":
                    create_survival_table(sqlite_path)
                case "patient":
                    create_patient_table(sqlite_path)
                case "stain":
                    create_stain_table(sqlite_path)
                case _:
                    raise RuntimeError(f"Unkown table name {table_name}")

# Storage specific tables
def create_ingress_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "ingress" table
        cursor.execute('''
        CREATE TABLE ingress (
            hash TEXT,
            filepath TEXT,
            filename TEXT,
            UNIQUE(hash, filepath, filename)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)

def create_storage_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "storage" table
        cursor.execute('''
        CREATE TABLE storage (
            hash TEXT NOT NULL PRIMARY KEY,
            filepath TEXT,
            filename TEXT,
            scanname TEXT,
            FOREIGN KEY(hash) REFERENCES ingress(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)

# Task specific tables
def create_myc_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "myc" table
        cursor.execute('''
        CREATE TABLE myc (
            hash TEXT NOT NULL PRIMARY KEY,
            value TEXT,
            FOREIGN KEY(hash) REFERENCES storage(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)
    
def create_diagnosis_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "diagnosis" table
        cursor.execute('''
        CREATE TABLE diagnosis (
            hash TEXT NOT NULL PRIMARY KEY,
            value TEXT,
            FOREIGN KEY(hash) REFERENCES storage(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)
    
def create_survival_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "survival" table
        cursor.execute('''
        CREATE TABLE survival (
            hash TEXT NOT NULL PRIMARY KEY,
            value TEXT,
            FOREIGN KEY(hash) REFERENCES storage(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)
    
# metadata specific tables
def create_stain_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "stain" table
        cursor.execute('''
        CREATE TABLE stain (
            hash TEXT NOT NULL PRIMARY KEY,
            value TEXT,
            FOREIGN KEY(hash) REFERENCES storage(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)

def create_patient_table(sqlite_path):
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Create the "stain" table
        cursor.execute('''
        CREATE TABLE patient (
            hash TEXT NOT NULL PRIMARY KEY,
            value TEXT,
            FOREIGN KEY(hash) REFERENCES storage(hash)
        )
        ''')
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as error:
        raise RuntimeError(error)
