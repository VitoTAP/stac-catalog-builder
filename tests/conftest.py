from pathlib import Path

import pytest


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture
def geotiff_input_dir(data_dir) -> Path:
    return data_dir / "geotiff" / "mock-geotiffs"
