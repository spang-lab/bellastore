# Bellatrix Storage

Organizing whole slide image scans (one of the main data sources in digital pathology) by automatically creating a storage and keeping track of new data via an sqlite database.
This database is particularly helpful when querying specific slides for downstream tasks
via our custom digital pathology API `bellapi`.

## Installation

The source code is currently hosted under [https://github.com/spang-lab/bellastore](https://github.com/spang-lab/bellastore).

Binary installers are available at PyPi.

```sh
pip install bellastore
```

## Usage

Installing the package will automatically install the binaries for the two main scripts.
- `bellastore-insert` inserts new scans from ingress to storage
- `bellastore-backup` backups the sqlite database

```sh
# For dry run (scans will not be moved and database will not be changed)
bellastore-insert --root_dir <directory holding storage and backup> \ 
                        --ingress_dir <directory_holding_new_scans> \
                        --sqlite_name <name of sqlite database>

# Moving slides and recording in database
bellastore-insert --root_dir <directory holding storage> \ 
                        --ingress_dir <directory_holding_new_scans> \
                        --sqlite_name <name of sqlite database> \
                        --move

# Create backup of database in backup directory
bellastore-backup --root_dir <directory holding storage and backup> \
                            --sqlite_name <name of sqlite database>
```

## Documentation

Along with the [source code](https://github.com/spang-lab/bellastore), under `docs/demo.ipynb` we provide a demo of the main usecase of the package, that leads you trough the steps of the main integration test `tests/test_db_fs.py::test_classic`.\
The package comes with a test suite, serving to demonstrate the functionality of each unit of the package.


