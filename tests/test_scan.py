import pytest
from os.path import join as _j
from bellastore.utils.scan import Scan


# --------------- #
# --- S C A N --- #
# --------------- #

def test_scan(ndpi_scan):
    """Tests the Scan initialization"""
    scan = Scan(path = ndpi_scan)
    assert scan.path == ndpi_scan
    assert scan.filename == 'test_scan'

def test_is_valid_scan(ndpi_scan, txt_scan):
    """Test if a valid scan is valid"""
    scan = Scan(path = ndpi_scan)
    invalid_scan = Scan(path = txt_scan)
    assert scan.is_valid()
    assert not invalid_scan.is_valid()

def test_hashing(ndpi_scan):
    """Test if the hasing works"""
    scan = Scan(path = ndpi_scan)
    scan.state.move_forward()
    hash = scan.hash_scan()
    assert hash