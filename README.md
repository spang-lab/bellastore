# Bellatrix Storage

This python package organizes whole slide image scans by automatically creating a storage and keeps track of new entries via sqlite databases.

These databases are particularly helpful when querying specific slides for downstream tasks
via our custom API bellapi.

The package mainly abstracts the file system with the `Fs` class and builds the database class `Db` around it.

The package comes with an extensive test suite, serving to demonstrate the functionality of each unit of the package.

Under `demo.ipynb` we also provide a demo of the main usecase of the package, that leads you trough the steps of the main integration test `tests/test_db_fs.py::test_classic`

