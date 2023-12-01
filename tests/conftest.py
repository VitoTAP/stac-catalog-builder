from pathlib import Path

import pytest

# DATA_DIR = Path(__file__) / "data"


@pytest.fixture
def data_dir():
    return Path(__file__).parent / "data"


@pytest.fixture
def test_output_dir():
    return Path(__file__).parent.parent / "tmp"