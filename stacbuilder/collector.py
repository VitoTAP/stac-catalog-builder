import datetime as dt
from itertools import islice
from pathlib import Path
from typing import Iterable, List, Optional, Protocol, Tuple, Union
from openeo.util import normalize_crs
import rasterio

from stactools.core.io import ReadHrefModifier

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.metadata import AssetMetadata, BandMetadata, RasterMetadata
from stacbuilder.pathparsers import InputPathParser, InputPathParserFactory
from stacbuilder.projections import reproject_bounding_box, project_polygon


class IDataCollector(Protocol):
    """Interface/Protocol for all DataCollector implementations."""

    def collect(self) -> None:
        """Collect the data and store it internally.

        Each implementation needs to add a method to access the collected data,
        because a specific method will be more clear that what we could add here.

        At the level of this class here we could only add a method that returns
        `Any`, and with a generic name that will be a bit too vague.
        """
        ...

    def has_collected(self) -> bool:
        """Has the collection been done/ does the collector contain any data."""
        ...

    def reset(self) -> None:
        """Empty the collected data."""
        ...


class FileCollector(IDataCollector):
    """Collects files that match a glob, in a directory.

    Note that the values None and [] have a different meaning for self.input_files:
    See :meth: FileCollector.has_collected
    """

    def __init__(
        self, input_dir: Optional[Path] = None, glob: Optional[str] = "*", max_files: Optional[int] = -1
    ) -> None:
        #
        # Settings: these are just data, not components we delegate work to.
        #
        # These are public members, or should have a public property.
        self.input_dir: Path = input_dir
        self.glob: str = glob
        self.max_files: int = max_files
        self._set_missing_fields_to_defaults()

        # The result
        self._input_files: List[Path] = None

    def _set_missing_fields_to_defaults(self):
        if not self.input_dir:
            self.input_dir = None

        if not self.glob:
            self.glob = "*"

        if not self.max_files:
            self.max_files = -1

    @staticmethod
    def from_config(config: FileCollectorConfig) -> "FileCollector":
        """Use the configuration object to create a new FileCollector instance."""
        collector = FileCollector()
        collector.setup(config)
        return collector

    def setup(self, config: FileCollectorConfig):
        """Read the settings for this instance from the configuration object."""
        self.input_dir = config.input_dir
        self.glob = config.glob
        self.max_files = config.max_files
        self._set_missing_fields_to_defaults()
        self.reset()

    def collect(self):
        input_files = (f for f in self.input_dir.glob(self.glob) if f.is_file())

        if self.max_files > 0:
            input_files = islice(input_files, self.max_files)

        self._input_files = list(input_files)

    def has_collected(self) -> bool:
        """Whether or not the data has been collected.

        Note that the values None and [] have a different meaning here:
        Namely:
            `self.input_files == []`
            means no files were found during collection,
        but in contrast:
            `self.input_files is None`
            means the collection was never run in the first place.
        """
        return self._input_files is not None

    def reset(self):
        """Reset the collector: clear the collected files."""
        self._input_files = None

    @property
    def input_files(self) -> List[Path]:
        """Get the collected input files."""
        return self._input_files or []


class IMetadataCollector(IDataCollector):
    """Interface/Protocol for collector that gets Metadata objects from a source.

    You need still to implement the method `collect`.

    Note that the values None and [] have a different meaning for self.metadata_list,
    See :meth: IMetadataCollector.has_collected
    """

    def __init__(self):
        self._metadata_list: List[AssetMetadata] = None

    def has_collected(self) -> bool:
        """Whether or not the data has been collected.

        Note that the values None and [] have a different meaning here:
        Namely:
            `self.metadata_list == []`
            means no asset metadata were found during collection,
        but in contrast:
            `self.metadata_list is None`
            means the collection was never run in the first place.
        """
        return self._metadata_list is not None

    def reset(self):
        """Reset the collector: clear the collected asset metadata,

        Value must be `None`, as if it was never run.
        """
        self._metadata_list = None

    @property
    def metadata_list(self) -> List[AssetMetadata]:
        return self._metadata_list


class MapGeoTiffToAssetMetadata:
    """Extracts AssetMetadata from each GeoTIFF file.

    TODO: VVVV move functionality to GeoTiffMetadataCollector
    """

    def __init__(
        self,
        path_parser: InputPathParser,
        href_modifier: Optional[ReadHrefModifier] = None,
    ) -> None:
        # Store dependencies: components that have to be provided to constructor
        self._path_parser = path_parser
        # TODO: remove MakeRelativeToCollection as default. This is a hack to test it quickly.
        self._href_modifier = href_modifier or None

    def to_metadata(
        self,
        asset_path: Union[Path, str],
    ) -> AssetMetadata:
        if not asset_path:
            raise ValueError(
                f'Argument "asset_path" must have a value, it can not be None or the empty string. {asset_path=}'
            )
        if not isinstance(asset_path, (Path, str)):
            raise TypeError(f'Argument "asset_path" must be of type Path or str. {type(asset_path)=}, {asset_path=}')

        asset_meta = AssetMetadata(extract_href_info=self._path_parser)
        asset_meta.original_href = asset_path
        asset_meta.asset_path = asset_path
        asset_meta.asset_id = Path(asset_path).stem
        asset_meta.item_id = Path(asset_path).stem
        asset_meta.datetime = dt.datetime.utcnow()  # TODO VVVV Why do we take the current time?

        if self._href_modifier:
            asset_meta.href = self._href_modifier(asset_path)
        else:
            asset_meta.href = asset_path

        with rasterio.open(asset_path) as dataset:
            asset_meta.shape = dataset.shape

            # Get the EPSG code of the dataset
            proj_epsg = None
            normalized_epsg = normalize_crs(dataset.crs)
            if normalized_epsg is not None:
                proj_epsg = normalized_epsg
            elif hasattr(dataset.crs, "to_epsg"):
                proj_epsg = dataset.crs.to_epsg()

            if not proj_epsg:
                proj_epsg = 4326

            # Get the projected bounding box of the dataset
            asset_meta.bbox_projected = BoundingBox.from_list(list(dataset.bounds), epsg=proj_epsg)

            # Get the transform of the dataset
            asset_meta.transform = list(dataset.transform)[0:6]

            # Project the geometry to EPSG:4326 (latlon) and get the bounding box
            asset_meta.geometry_lat_lon = project_polygon(
                geometry=asset_meta.geometry_proj, from_crs=proj_epsg, to_crs=4326
            )
            asset_meta.bbox_lat_lon = BoundingBox.from_list(asset_meta.geometry_lat_lon.bounds, epsg=4326)

            bands = []
            tags = dataset.tags() or {}
            units = tags.get("units")
            for i in range(dataset.count):
                # TODO: if tags contains unit, add the unit
                band_md = BandMetadata(data_type=dataset.dtypes[i], index=i, nodata=dataset.nodatavals[i], units=units)
                bands.append(band_md)

            # TODO: RasterMetadata.shape is duplicate info. Eliminate RasterMetadata and use BandMetadata directly
            #   RasterMetadata.shape is duplicate info and the only other property left in RasterMetadata are the bands.
            raster_meta = RasterMetadata(shape=dataset.shape, bands=bands)
            # TODO: Decide: do we really need the raster tags.
            asset_meta.tags = dataset.tags()
            asset_meta.raster_metadata = raster_meta

        asset_meta.process_href_info()

        file_stat = asset_path.stat()
        asset_meta.file_size = file_stat.st_size

        return asset_meta


class CreateAssetUrlFromPath:
    """Implements stactools.core.io.ReadHrefModifier"""

    def __init__(self, href_template: str, data_root: Path) -> None:
        self.url_template = href_template
        self.data_root = Path(data_root)

    def __call__(self, asset_path: Path) -> str:
        """This method must match the signature of ReadHrefModifier.
        ReadHrefModifier is a type alias for Callable[[str], str]
        """
        return self.get_url(asset_path)

    def get_url(self, asset_path: Path):
        rel_path: Path = asset_path.relative_to(self.data_root)
        return self.url_template.format(str(rel_path))


class GeoTiffMetadataCollector(IMetadataCollector):
    def __init__(
        self,
        collection_config: CollectionConfig,
        file_collector: FileCollector,
    ):
        super().__init__()

        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if file_collector is None:
            raise ValueError('Argument "file_collector" can not be None, must be a FileCollector instance.')

        # Components / dependencies that must be provided
        self._file_collector = file_collector

        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config

        # Components / dependencies that we set up internally
        self._geotiff_to_metadata_mapper: MapGeoTiffToAssetMetadata = None
        self._path_parser: InputPathParser = None

        self._setup_internals()

    def _setup_internals(self):
        href_modifier = None
        collection_conf = self._collection_config

        cfg_href_modifier = collection_conf.asset_href_modifier
        if cfg_href_modifier:
            href_modifier = CreateAssetUrlFromPath(
                data_root=cfg_href_modifier.data_root, href_template=cfg_href_modifier.url_template
            )

        path_parser = None
        if collection_conf.input_path_parser:
            path_parser = InputPathParserFactory.from_config(collection_conf.input_path_parser)

        self._geotiff_to_metadata_mapper = MapGeoTiffToAssetMetadata(
            path_parser=path_parser, href_modifier=href_modifier
        )

    @staticmethod
    def from_config(
        collection_config: CollectionConfig,
        file_coll_cfg: FileCollectorConfig,
    ) -> "GeoTiffMetadataCollector":
        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if file_coll_cfg is None:
            raise ValueError('Argument "file_coll_cfg" can not be None, must be a FileCollectorConfig instance.')

        file_collector = FileCollector.from_config(file_coll_cfg)

        return GeoTiffMetadataCollector(
            collection_config=collection_config,
            file_collector=file_collector,
        )

    @property
    def collection_config(self) -> CollectionConfig:
        return self._collection_config

    @property
    def file_collector(self) -> FileCollector:
        return self._file_collector

    @property
    def path_parser(self) -> InputPathParser:
        return self._path_parser

    @property
    def geotiff_to_metadata_mapper(self) -> MapGeoTiffToAssetMetadata:
        return self._geotiff_to_metadata_mapper

    def reset(self) -> None:
        super().reset()
        self._file_collector.reset()

    def get_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        if not self._file_collector.has_collected():
            self._file_collector.collect()

        for file in self._file_collector.input_files:
            yield file

    def collect(self) -> None:
        self._metadata_list = []

        import asyncio
        loop = asyncio.get_event_loop()

        async def fetch_metadata(file):
            return await loop.run_in_executor(None, self._geotiff_to_metadata_mapper.to_metadata, file)

        task_list = []
        for file in self.get_input_files():
            task_list.append( fetch_metadata(file))
        self._metadata_list = loop.run_until_complete(asyncio.gather( *task_list))


