from bellastore.database.db import Db
import argparse

def main():
    cli = argparse.ArgumentParser()
    cli.add_argument(
        '--root_dir', type = str, default = '/data/deep-learning/test',
       help = 'Directory where sqlite and storage will be initialized under, in particular root_dir/storage/scans.sqlite'
    )
    cli.add_argument(
        '--ingress_dir', type = str, default = '/data/deep-learning/test/slides',
        help = 'Directory holding scans to be inserted into storage.'
    )
    cli.add_argument(
        '--sqlite_name', type = str, default = 'scans.sqlite',
        help = 'Name of the sqlite database file'
    )
    cli.add_argument(
        '--verbose', action=argparse.BooleanOptionalAction,
        help = 'This will log the whole content of the database and the storage which might clutter the output'
    )
    cli.add_argument(
        '--move', action=argparse.BooleanOptionalAction,
        help = 'This needs to be explicitly set in order to mess with the filesystem, otherwise only dry run will be done.'
    )

    args = cli.parse_args()
    root_dir = args.root_dir
    ingress_dir = args.ingress_dir
    sqlite_name = args.sqlite_name
    verbose = args.verbose
    move = args.move
    

    db = Db(root_dir, ingress_dir, sqlite_name)

    if move:
        db.insert_from_ingress()
        if verbose:
            print('Done, your final storage looks like:')
            print(str(db))
    else:
        db = Db(root_dir, ingress_dir, sqlite_name)
        if verbose:
            print('The storage currently looks like this')
            print(str(db))
        valid_scans = db.get_valid_scans_from_ingress()
        formats = set([scan.filename.split('.')[1] for scan in valid_scans])
        print(formats)
        print(f'Total amount of valid scans to be moved {len(valid_scans)}.')
        print(f'Formats present {formats}')


if __name__ == '__main__':
    main()