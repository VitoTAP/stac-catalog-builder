from itertools import islice
from pathlib import Path
from typing import Iterable, List, Literal, Optional, Protocol

from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.mapper import MapGeoTiffToAssetMetadata
from stacbuilder.metadata import AssetMetadata

FileType = Literal["GeoTiff"]


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

    def __init__(self, input_dir: Optional[Path] = None, glob: str = "*", max_files: int = -1) -> None:
        self.input_dir: Optional[Path] = input_dir
        self.glob: str = glob or "*"
        self.max_files: int = max_files if max_files is not None else -1
        self._input_files: Optional[List[Path]] = None

    @staticmethod
    def from_config(config: FileCollectorConfig) -> "FileCollector":
        """Create a FileCollector from a FileCollectorConfig."""
        return FileCollector(
            input_dir=config.input_dir,
            glob=config.glob or "*",
            max_files=config.max_files if config.max_files is not None else -1,
        )

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


class MetadataCollector(IMetadataCollector):
    """
    Class responsible for collecting AssetMetadata from input files.
    It uses a FileCollector to gather the files and a Mapper to convert files to AssetMetadata.
    Currently, it supports GeoTIFF files and maps them to AssetMetadata using MapGeoTiffToAssetMetadata.
    """

    def __init__(
        self,
        file_collector: FileCollector,
        metadata_mapper: MapGeoTiffToAssetMetadata,
    ):
        super().__init__()
        if file_collector is None:
            raise ValueError('Argument "file_collector" can not be None, must be a FileCollector instance.')

        if metadata_mapper is None:
            raise ValueError(
                'Argument "metadata_mapper" can not be None, must be a MapGeoTiffToAssetMetadata instance.'
            )

        self._file_collector = file_collector
        self._metadata_mapper = metadata_mapper

    @staticmethod
    def from_config(
        collection_config: CollectionConfig,
        file_coll_cfg: FileCollectorConfig,
        file_type: FileType = "GeoTiff",
    ) -> "MetadataCollector":
        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if file_coll_cfg is None:
            raise ValueError('Argument "file_coll_cfg" can not be None, must be a FileCollectorConfig instance.')

        if not isinstance(file_type, str):
            raise TypeError(f'Argument "file_type" must be a str, not {type(file_type)}.')

        file_collector = FileCollector.from_config(config=file_coll_cfg)

        match file_type.lower():
            case "geotiff" | "geotif" | "tiff" | "tif":
                metadata_mapper = MapGeoTiffToAssetMetadata.from_config(collection_config=collection_config)
            case _:
                raise ValueError(f'Unsupported file type: {file_type}. Supported types: ["GeoTiff"]')

        return MetadataCollector(file_collector=file_collector, metadata_mapper=metadata_mapper)

    @property
    def file_collector(self) -> FileCollector:
        return self._file_collector

    @property
    def metadata_mapper(self) -> MapGeoTiffToAssetMetadata:
        return self._metadata_mapper

    def reset(self) -> None:
        super().reset()
        self._file_collector.reset()

    def get_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        if not self.file_collector.has_collected():
            self.file_collector.collect()

        for file in self.file_collector.input_files:
            yield file

    def collect(self) -> None:
        self._metadata_list = []

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.metadata_mapper.to_metadata, file) for file in self.get_input_files()]
            self._metadata_list = [future.result() for future in futures]
