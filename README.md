# Bellatrix Storage

Organizing whole slide image scans (one of the main data sources in digital pathology) by automatically creating a storage and keeping track of new data via an sqlite database.
This database is particularly helpful when querying specific slides for downstream tasks
via our custom API bellapi.

## Installation

```sh
# After cloning the repository
pip install -e .
```

## Usage

```sh
# For dry run (scans will not be moved)
python3 scripts/main.py --root_dir <directory holding storage and backup> \ 
                        --ingress_dir <directory_holding_scans> \
                        --sqlite_name <name of sqlite database>

# Moving slides
python3 scripts/main.py --root_dir <directory holding storage> \ 
                        --ingress_dir <directory_holding_scans> \
                        --sqlite_name <name of sqlite database> \
                        --move

# Create backup of database in backup directory
python3 scripts/backup.py --root_dir <directory holding storage and backup> \
                            --sqlite_name <name of sqlite database>
```

## Documentation

Under [docs/demo.ipynb](docs/demo.ipynb) we provide a demo of the main usecase of the package, that leads you trough the steps of the main integration test [tests/test_db_fs.py::test_classic](tests/test_db_fs.py::test_classic).\
The package comes with an extensive test suite, serving to demonstrate the functionality of each unit of the package.


