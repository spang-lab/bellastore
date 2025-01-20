import pytest

from bellastore.utils.scan import Scan
from bellastore.utils.state import State

# ----------------- #
# --- S T A T E --- #
# ----------------- #

def test_state():
    """Tests the State initialization"""
    state = State()
    

def test_is_valid():
    """Tests if a simple state is valid"""
    state = State()
    assert state.is_valid()

def test_is_valid_invalid():
    """Tests if a invalid state in fact is invalid"""
    state = State()
    state.states["hashed"] = True
    assert not state.is_valid()

def test_has_state():
    """Tests if the has_state method works"""
    state = State()
    assert state.has_state("none")
    assert not state.has_state("hashed")

def test_get_state():
    """Tests the get_state method"""
    state = State()
    assert "none" == state.get_state()

def test_move_forward():
    """Tests the move_forward method in combination with others"""
    state = State()
    state.move_forward()
    assert "none" != state.get_state()
    assert "valid" == state.get_state()
    assert state.has_state("none")
    assert state.has_state("valid")
    assert not state.has_state("hashed")