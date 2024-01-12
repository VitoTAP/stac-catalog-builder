from pathlib import Path

import pytest

# DATA_DIR = Path(__file__) / "data"


@pytest.fixture
def data_dir():
    return Path(__file__).parent / "data"


@pytest.fixture
def collection_output_dir():
    return Path(__file__).parent.parent / "tmp"


@pytest.fixture
def geotiff_input_dir(data_dir):
    return data_dir / "geotiff" / "mock-geotiffs"
