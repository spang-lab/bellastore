# Bellatrix Storage

This python package organizes whole slide image scans (one of the main data sources in digital pathology) by automatically creating a storage and keeping track of new data via an sqlite database.

This database is particularly helpful when querying specific slides for downstream tasks
via our custom API bellapi.

The package abstracts the file system with the `Fs` class and builds the database class `Db` around it.

Under [docs/demo.ipynb](docs/demo.ipynb) we also provide a demo of the main usecase of the package, that leads you trough the steps of the main integration test [tests/test_db_fs.py::test_classic](tests/test_db_fs.py::test_classic).\
The package comes with an extensive test suite, serving to demonstrate the functionality of each unit of the package.

We provide a [main script](main.py) allowing to easily use bellastore from the command line.
The script provides the `--move` flag which allows for a dry run.
Furthermore, we provide backup functionality with the [backup script](backup.py) in order easily backup the database.


