from bellastore.database.db import Db
import argparse

def main():
    cli = argparse.ArgumentParser()
    cli.add_argument(
        'root_dir', type = str, default = '/data/deep-learning/test',
       help = 'Directory where sqlite and storage will be initialized under, i.e. root_dir/storage'
    )
    cli.add_argument(
        '--ingress_dir', type = str, default = '/data/deep-learning/test/slides',
        help = 'Directory holding scans'
    )
    cli.add_argument(
        '--sqlite_name', type = str, default = 'scans.sqlite',
        help = 'Name of the sqlite file'
    )
    args = cli.parse_args()
    root_dir = args.root_dir
    ingress_dir = args.ingress_dir
    sqlite_name = args.sqlite_name

    db = Db(root_dir, ingress_dir, sqlite_name)
    db.insert_from_ingress()