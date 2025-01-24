import pytest
from os.path import join as _j
from pathlib import Path
from bellastore.utils.scan import Scan


# Helpers
def create_scan_file(dir, filename: str):
    path = _j(dir, filename)
    open(path, 'w').close()
    return path

def get_all_files(dir):
    files = Path(dir).rglob("*")
    file_paths = list(files)
    file_paths = [str(file_path) for file_path in file_paths]
    return file_paths

# Fixtures in this module's scope (rest is in conftest)
@pytest.fixture(scope="function")
def ndpi_scan_path(root_dir):
    return create_scan_file(root_dir, 'test_scan.ndpi')

@pytest.fixture(scope="function")
def txt_scan_path(root_dir):
    return create_scan_file(root_dir, 'test_scan.txt')

@pytest.fixture(scope="function")
def ndpi_scan(root_dir):
        p = create_scan_file(root_dir, 'test_scan.ndpi')
        return Scan(str(p))

@pytest.fixture(scope="function")
def target_dir(root_dir):
    sub = root_dir / "sub"
    sub.mkdir()
    return sub


def test_scan(ndpi_scan_path):
    """Tests the Scan initialization"""
    scan = Scan(path = ndpi_scan_path)
    assert scan.path == ndpi_scan_path
    assert scan.filename == 'test_scan.ndpi'
    assert scan.scanname == 'test_scan'

def test_is_valid_scan(ndpi_scan_path, txt_scan_path):
    """Test if a valid scan is valid"""
    scan = Scan(path = ndpi_scan_path)
    invalid_scan = Scan(path = txt_scan_path)
    assert scan.is_valid()
    assert not invalid_scan.is_valid()

def test_hashing(ndpi_scan_path):
    """Test if the hasing works"""
    scan = Scan(path = ndpi_scan_path)
    hash = scan.hash_scan()
    print(hash)
    assert hash

def test_move_scan(ndpi_scan, target_dir):
    ndpi_scan.move(target_dir)
    assert [ndpi_scan.path] == get_all_files(target_dir)
