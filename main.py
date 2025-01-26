from bellastore.database.db import Db
import argparse

def main():
    cli = argparse.ArgumentParser()
    cli.add_argument(
        '--root_dir', type = str, default = '/data/deep-learning/test',
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
    cli.add_argument(
        '--move', action=argparse.BooleanOptionalAction,
        help = 'This needs to be explicitly set in order to mess with the filesystem, otherwise only dry run will be done.'
    )
    args = cli.parse_args()
    root_dir = args.root_dir
    ingress_dir = args.ingress_dir
    sqlite_name = args.sqlite_name
    move = args.move

    db = Db(root_dir, ingress_dir, sqlite_name)
    print(str(db))

    if(move):
        db.insert_from_ingress()
        print('Done, your final storage looks like:')
        print(str(db))
    else:
        db = Db(root_dir, ingress_dir, sqlite_name)
        valid_scans = db.get_valid_scans_from_ingress()
        if valid_scans:
            print('\nSummary of valid scans:')
            for scan in valid_scans:
                print(scan.path)
        else:
            print('\nNo valid scans.')


if __name__ == '__main__':
    main()