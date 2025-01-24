import os
from os.path import join as _j
import sqlite3

from bellastore.database.db import Db


def table_exists(sqlite_path, table_name: str):
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table_name, ))
        result = cursor.fetchall()
    if result:
        return True
    else:
        False

def check_tables_exists(sqlite_path):
    for table_name in ['ingress', 'storage']:
        table_exists(sqlite_path, table_name)


def test_initialization(root_dir, ingress_dir):
    db = Db(root_dir, ingress_dir, 'scans.sqlite')
    check_tables_exists(db.sqlite_path)

    