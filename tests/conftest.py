import os
import pytest
    


@pytest.fixture(scope="session")
def ndpi_scan(tmp_path_factory):
    '''Create empty ndpi scan called test_scan_ndpi and returns its path'''
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.ndpi")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path
    os.remove(tmp_file_path)

@pytest.fixture(scope="session")
def txt_scan(tmp_path_factory):
    '''Create empty (invalid) txt scan called test_scan_ndpi and returns its path'''
    tmp_file_path = os.path.join(tmp_path_factory.mktemp("data"),"test_scan.txt")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path

@pytest.fixture(scope="session")
def filesystem(tmp_path_factory):
    root_dir = tmp_path_factory.mktemp("root")
    storage_dir = os.path.join(root_dir, "storage")
    scan_1_dir = os.path.join(root_dir, "ingress", "scan_1")
    scan_2_dir = os.path.join(root_dir, "ingress", "scan_2")
    # Define directory structure
    dirs = [
        storage_dir,
        scan_1_dir,
        scan_2_dir
    ]
    # Create all directories
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
    scan_1_path = os.path.join(scan_1_dir, "test_scan_1.ndpi")
    open(scan_1_path, 'w').close()  
    scan_2_path = os.path.join(scan_2_dir, "test_scan_2.ndpi")
    open(scan_2_path, 'w').close()
    yield root_dir



