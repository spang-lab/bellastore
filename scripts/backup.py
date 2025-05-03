from bellastore.database.db import Db
import argparse

def main():
    cli = argparse.ArgumentParser()
    cli.add_argument(
        '--root_dir', type = str, default = '/data/deep-learning/storage',
       help = 'Directory where sqlite and storage will be initialized under, in particular root_dir/storage/scans.sqlite'
    )
    cli.add_argument(
        '--sqlite_name', type = str, default = 'scans.sqlite',
        help = 'Name of the sqlite database file'
    )
    args = cli.parse_args()
    root_dir = args.root_dir
    sqlite_name = args.sqlite_name

    db = Db(root_dir=root_dir, ingress_dir=None, filename=sqlite_name)

    db.setup_logging()
    db.create_backup()


if __name__ == '__main__':
    main()