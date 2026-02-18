import threading
from math import log10
from pathlib import Path
from typing import ClassVar, Optional, Union

import rasterio
from loguru import logger
from openeo.util import normalize_crs
from pystac.media_type import MediaType
from rio_cogeo import cog_validate
from upath.implementations.cloud import S3Path

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.config import AssetHrefModifierConfig, CollectionConfig
from stacbuilder.metadata import AssetMetadata, BandMetadata
from stacbuilder.pathparsers import InputPathParser, InputPathParserFactory


class CreateAssetUrlFromPath:
    """Implements stactools.core.io.ReadHrefModifier"""

    def __init__(self, href_template: str, data_root: Path) -> None:
        self.url_template = href_template
        self.data_root = Path(data_root)

    @classmethod
    def from_config(cls, config: Optional[AssetHrefModifierConfig]) -> "CreateAssetUrlFromPath":
        """
        Create a CreateAssetUrlFromPath from a CollectionConfig.
        returns None if no asset_href_modifier is configured.
        """
        if not config:
            return None

        return cls(
            href_template=config.url_template,
            data_root=Path(config.data_root),
        )

    def __call__(self, asset_path: Path) -> str:
        """This method must match the signature of ReadHrefModifier.
        ReadHrefModifier is a type alias for Callable[[str], str]
        """
        return self.get_url(asset_path)

    def get_url(self, asset_path: Path):
        rel_path: Path = asset_path.relative_to(self.data_root)
        return self.url_template.format(str(rel_path))


class MapGeoTiffToAssetMetadata:
    """
    Class to extract AssetMetadata from GeoTIFF files.
    """

    # Cache COG validation result per asset type to avoid repeated expensive checks across threads.
    _media_type_cache: ClassVar[dict[str, MediaType]] = {}
    _media_type_cache_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        path_parser: InputPathParser,
        href_modifier: Optional[CreateAssetUrlFromPath],
    ) -> None:
        # Store dependencies: components that have to be provided to constructor
        self._path_parser = path_parser

        self._href_modifier: Optional[CreateAssetUrlFromPath] = href_modifier

    @classmethod
    def from_config(
        cls,
        collection_config: CollectionConfig,
    ) -> "MapGeoTiffToAssetMetadata":
        """Create a MapGeoTiffToAssetMetadata from a CollectionConfig."""
        if not collection_config:
            raise ValueError("CollectionConfig must not be None")

        path_parser = InputPathParserFactory.from_config(collection_config.input_path_parser)
        href_modifier = CreateAssetUrlFromPath.from_config(config=collection_config.asset_href_modifier)

        return cls(path_parser=path_parser, href_modifier=href_modifier)

    def process_href_info(self, href: str) -> dict[str, str]:
        """Uses the path parser to extract information from the href.
        :param href: The href to process.
        :return: A dictionary with the extracted information.
        """
        if self._path_parser is None:
            return {}
        return self._path_parser.parse(href)

    def to_metadata(
        self,
        asset_path: Union[Path, str],
    ) -> AssetMetadata:
        """Extracts AssetMetadata from a GeoTIFF file.
        :param asset_path: Path to the GeoTIFF file, can be a string or a Path object.
        :return: AssetMetadata object containing metadata extracted from the GeoTIFF file.
        :raises ValueError: If asset_path is None or empty.
        :raises TypeError: If asset_path is not a Path or str.
        """
        if not asset_path:
            raise ValueError(
                f'Argument "asset_path" must have a value, it can not be None or the empty string. {asset_path=}'
            )
        if isinstance(asset_path, str):
            asset_path = Path(asset_path)

        if not isinstance(asset_path, (Path, str)):
            raise TypeError(f'Argument "asset_path" must be of type Path or str. {type(asset_path)=}, {asset_path=}')

        if self._href_modifier:
            modified_href = self._href_modifier(asset_path)
        else:
            modified_href = asset_path.as_posix()

        # check for s3 path and adjust the file path.
        if isinstance(asset_path, S3Path):
            _asset_path = asset_path.as_uri()
        else:
            _asset_path = asset_path

        with rasterio.open(_asset_path) as dataset:
            assert isinstance(dataset, rasterio.DatasetReader), "Dataset must be a rasterio DatasetReader"
            shape = dataset.shape

            # Get the EPSG code of the dataset

            normalized_epsg = normalize_crs(dataset.crs)
            if normalized_epsg is not None:
                proj_epsg = normalized_epsg
            elif hasattr(dataset.crs, "to_epsg"):
                proj_epsg = dataset.crs.to_epsg()
            else:
                proj_epsg = 4326  # Default to WGS 84 if no EPSG code is found

            # round of values defined in terms of number of digits to keep
            # after decimals, which is defined to be between 0.1-1.0% of the resolution.
            # default is set to 6 decimals (approx single point float-precision)
            if isinstance(dataset.res[0], (float, int)):
                precision = abs(int(log10(abs(dataset.res[0] * 0.001))))
            else:
                precision = 6  # default

            # Get the projected bounding box of the dataset
            _bounds = [round(_bo, precision) for _bo in list(dataset.bounds)]  # round off data
            bbox_projected = BoundingBox.from_list(_bounds, epsg=proj_epsg)

            # Get the transform of the dataset
            _transform = [round(_bo, precision) for _bo in list(dataset.transform)[:6]]  # round off data
            transform = list(_transform)[0:6]

            bands = []
            tags = dataset.tags() or {}
            units = tags.get("units")
            for i in range(dataset.count):
                band_md = BandMetadata(data_type=dataset.dtypes[i], index=i, nodata=dataset.nodatavals[i], units=units)
                bands.append(band_md)

        file_stat = asset_path.stat()

        href_info = self.process_href_info(str(asset_path))
        asset_type = href_info.get("asset_type")

        media_type = self._resolve_media_type(asset_type=asset_type, asset_path=_asset_path)

        # Prepare the arguments for AssetMetadata, allowing href_info to override or add fields
        asset_metadata_args = dict(
            asset_path=asset_path,
            href=modified_href,
            original_href=str(asset_path),
            asset_id=Path(asset_path).stem,
            item_id=Path(asset_path).stem,
            shape=shape,
            proj_epsg=proj_epsg,
            bbox_projected=bbox_projected,
            transform=transform,
            bands=bands,
            file_size=file_stat.st_size,
            tags=tags,
            media_type=media_type,
        )
        asset_metadata_args.update(href_info)  # href_info can override or add fields

        asset_metadata = AssetMetadata(**asset_metadata_args)

        return asset_metadata

    def _resolve_media_type(self, asset_type: Optional[str], asset_path: Union[Path, str]) -> MediaType:
        """Return cached media type for an asset type or compute it once."""
        cache_key = asset_type or Path(asset_path).suffix
        cached = self._media_type_cache.get(cache_key)

        # If we have a cached media type for this asset type, return it immediately to avoid redundant COG validation.
        if cached:
            return cached
        with self._media_type_cache_lock:
            # Double-check the cache after acquiring the lock to prevent redundant COG validation in concurrent scenarios.
            cached = self._media_type_cache.get(cache_key)
            if cached:
                return cached

            if cog_validate(asset_path):
                media_type = MediaType.COG
                logger.info(f"Asset type {asset_type} confirmed to be valid COG's. Checked with {asset_path}.")
            else:
                logger.warning(
                    f"Asset {asset_path} of asset type {asset_type} is not a valid COG. Consider converting it to a COG for better performance.",
                )
                media_type = MediaType.GEOTIFF

            self._media_type_cache[cache_key] = media_type

            return media_type
