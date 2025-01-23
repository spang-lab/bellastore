import pytest
from os.path import join as _j
from bellastore.utils.scan import Scan


# Create empty scanner files
@pytest.fixture(scope="function")
def ndpi_scan_path(tmp_path_factory):
    '''Create empty (invalid) txt scan called test_scan_ndpi and returns its path'''
    tmp_file_path = _j(tmp_path_factory.mktemp("data"),"test_scan.ndpi")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path

@pytest.fixture(scope="function")
def txt_scan_path(tmp_path_factory):
    '''Create empty (invalid) txt scan called test_scan_ndpi and returns its path'''
    tmp_file_path = _j(tmp_path_factory.mktemp("data"),"test_scan.txt")
    open(tmp_file_path, 'w').close()
    yield tmp_file_path


# --------------- #
# --- S C A N --- #
# --------------- #

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
    scan.state.move_forward()
    hash = scan.hash_scan()
    print(hash)
    assert hash