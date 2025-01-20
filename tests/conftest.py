import os
import pytest

@pytest.fixture(scope="session")
def ndpi_scan(tmp_path_factory):
    "Create empty ndpi scan called test_scan_ndpi and returns its path"
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.ndpi")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path
    os.remove(tmp_file_path)

@pytest.fixture(scope="session")
def txt_scan(tmp_path_factory):
    "Create empty (invalid) txt scan called test_scan_ndpi and returns its path"
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.txt")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path
    os.remove(tmp_file_path)
