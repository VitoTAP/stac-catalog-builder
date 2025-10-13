from pathlib import Path
from typing import List

import numpy as np
import pytest
import rasterio
from rasterio.io import DatasetWriter

from stacbuilder.collector import IMetadataCollector
from stacbuilder.config import (
    CollectionConfig,
    FileCollectorConfig,
    InputPathParserConfig,
)
from stacbuilder.metadata import AssetMetadata


@pytest.fixture(scope="session")
def data_dir() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def geotiff_input_dir(data_dir) -> Path:
    return data_dir / "geotiff" / "mock-geotiffs"


@pytest.fixture(scope="session")
def collection_config_from_file(collection_config_path: Path) -> CollectionConfig:
    return CollectionConfig.from_json_file(collection_config_path)


@pytest.fixture(scope="session")
def collection_config_path(data_dir: Path) -> Path:
    return data_dir / "config" / "config-test-collection.json"


@pytest.fixture(scope="session")
def file_collector_config(geotiff_input_dir: Path) -> FileCollectorConfig:
    return FileCollectorConfig(input_dir=geotiff_input_dir, glob="*/*.tif")


@pytest.fixture(scope="session")
def collection_output_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("collection-no-groups")


@pytest.fixture(scope="session")
def grouped_collection_output_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("collections-per-group")


@pytest.fixture(scope="session")
def geotif_paths_relative() -> List[str]:
    return sorted(
        [
            "2000/observations_2m-temp-monthly_2000-01-01.tif",
            "2000/observations_2m-temp-monthly_2000-02-01.tif",
            "2000/observations_2m-temp-monthly_2000-03-01.tif",
            "2000/observations_tot-precip-monthly_2000-01-01.tif",
            "2000/observations_tot-precip-monthly_2000-02-01.tif",
            "2000/observations_tot-precip-monthly_2000-03-01.tif",
            "2001/observations_2m-temp-monthly_2001-01-01.tif",
            "2001/observations_2m-temp-monthly_2001-02-01.tif",
            "2001/observations_2m-temp-monthly_2001-03-01.tif",
            "2001/observations_tot-precip-monthly_2001-01-01.tif",
            "2001/observations_tot-precip-monthly_2001-02-01.tif",
            "2001/observations_tot-precip-monthly_2001-03-01.tif",
        ]
    )


@pytest.fixture(scope="session")
def geotif_paths_relative_small_set() -> List[str]:
    return sorted(
        [
            "2000/observations_2m-temp-monthly_2000-01-01.tif",
            "2000/observations_tot-precip-monthly_2000-01-01.tif",
        ]
    )


@pytest.fixture(scope="session")
def geotiff_paths(data_dir, geotif_paths_relative) -> List[Path]:
    input_dir = data_dir / "geotiff/mock-geotiffs"
    return generate_geotiff_paths(input_dir, geotif_paths_relative)


@pytest.fixture(scope="session")
def geotiff_paths_small_set(data_dir, geotif_paths_relative_small_set) -> List[Path]:
    input_dir = data_dir / "geotiff/mock-geotiffs"
    return generate_geotiff_paths(input_dir, geotif_paths_relative_small_set)


@pytest.fixture(scope="session")
def grouped_collection_test_config() -> CollectionConfig:
    data = {
        "collection_id": "foo-2023-v01_grouped",
        "title": "Foo collection with groups per year",
        "description": "Description of Foo",
        "instruments": [],
        "keywords": ["foo", "bar", "oof"],
        "mission": [],
        "platform": [],
        "providers": [
            {
                "name": "Test EO Org",
                "roles": ["licensor", "processor", "producer"],
                "url": "https://www.test-eo-org.nowhere.to.be.found.xyz/",
            }
        ],
        "input_path_parser": InputPathParserConfig(
            classname="MockPathParser",
        ),
        "item_assets": {
            "2m-temp-monthly": {
                "title": "2m temperature",
                "description": "temperature 2m above ground (Kelvin)",
                "eo_bands": [
                    {"name": "2m_temp", "description": "temperature 2m above ground (Kelvin)", "data_type": "uint16"}
                ],
            },
            "tot-precip-monthly": {
                "title": "total precipitation",
                "description": "total precipitation per month (m)",
                "eo_bands": [
                    {"name": "tot_precip", "description": "total precipitation per month (m)", "data_type": "uint16"}
                ],
            },
        },
    }
    return CollectionConfig(**data)


def generate_geotiff_paths(input_dir, geotif_paths_relative) -> List[Path]:
    return sorted(input_dir / f for f in geotif_paths_relative)


def create_geotiff_files(paths):
    for file in paths:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        create_mock_geotiff(file)


def create_mock_geotiff(tif_path: Path):
    """Create GeoTIFF raster files for testing.

    The pixels form a simple gradient just to have some data.

    This isn't being used in the tests, but is a utility to create mock data files.
    """
    # Based on the example in rasterio docs:
    # https://rasterio.readthedocs.io/en/stable/quickstart.html#opening-a-dataset-in-writing-mode
    x = np.linspace(-4.0, 4.0, 240)
    y = np.linspace(-3.0, 3.0, 180)
    X, Y = np.meshgrid(x, y)
    Z1 = np.exp(-2 * np.log(2) * ((X - 0.5) ** 2 + (Y - 0.5) ** 2) / 1**2)
    Z2 = np.exp(-3 * np.log(2) * ((X + 0.5) ** 2 + (Y + 0.5) ** 2) / 2.5**2)
    Z = 10.0 * (Z2 - Z1)

    dataset: DatasetWriter = rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=Z.shape[0],
        width=Z.shape[1],
        count=1,
        dtype=Z.dtype,
        crs=4326,
    )
    dataset.write(Z, 1)
    dataset.close()


class MockMetadataCollector(IMetadataCollector):
    """A mock implementation of IMetadataCollector.

    You give it some fixed AssetMetadata you want it to return.
    The `collect` method is a no-op in this case
    """

    def __init__(self, asset_metadata_list: List[AssetMetadata]):
        self._metadata_list = asset_metadata_list

    def add_asset(self, asset_md: AssetMetadata):
        if self._metadata_list is None:
            self._metadata_list = []
        self._metadata_list.append(asset_md)

    def collect(self) -> None:
        pass
