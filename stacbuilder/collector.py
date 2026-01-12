"""
This module defines classes for collecting Metadata from files. It uses a FileCollector to gather files and a Mapper to convert files to AssetMetadata.

It provides a MetadataCollector class that can be used to collect metadata from files matching a specified glob pattern in a directory.
It is designed to be flexible and can be extended to support different file types and metadata mapping strategies.
"""

from itertools import islice
from pathlib import Path
from queue import Queue
from typing import Any, Generator, Iterable, List, Literal, Optional, Protocol, Union

from loguru import logger
from upath import UPath

from stacbuilder.async_utils import AsyncTaskPoolMixin
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
        raise NotImplementedError("collect method must be implemented by subclasses.")

    def collect_stream(self) -> Generator[Any, None, None]:
        """Collect the data and yield it as a stream.

        Each implementation needs to add a method to access the collected data,
        because a specific method will be more clear that what we could add here.

        At the level of this class here we could only add a method that returns
        `Any`, and with a generic name that will be a bit too vague.
        """
        raise NotImplementedError("collect_stream method must be implemented by subclasses.")

    def has_collected(self) -> bool:
        """Has the collection been done/ does the collector contain any data."""
        raise NotImplementedError("has_collected method must be implemented by subclasses.")

    def reset(self) -> None:
        """Empty the collected data."""
        raise NotImplementedError("reset method must be implemented by subclasses.")


class FileCollector(IDataCollector):
    """Collects files that match a glob, in a directory.

    Note that the values None and [] have a different meaning for self.input_files:
    See :meth: FileCollector.has_collected
    """

    def __init__(self, input_dir: Optional[Path] = None, glob: str = "*", max_files: int = -1) -> None:
        """Initialize the FileCollector with an input directory, glob pattern, and maximum number of files.

        :param input_dir: The directory to search for files.
        :param glob: The glob pattern to match files. The default is "*", which matches all files.
        :param max_files: The maximum number of files to collect. Default is -1, which means no limit.
        """
        self.input_dir: Optional[Union[Path, UPath]] = input_dir
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

    def collect_stream(self) -> Iterable[Path]:
        """Get an iterator that collects files matching the glob pattern in the input directory and yields them one by one."""
        if self.input_dir is None:
            raise ValueError("Input directory is not set for FileCollector.")
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {self.input_dir}")
        logger.info(f"FileCollector: collecting files in '{self.input_dir}' with glob '{self.glob}'")
        input_files_gen = self.input_dir.glob(self.glob)

        if self.max_files > 0:
            input_files_gen = islice(input_files_gen, self.max_files)

        for file_path in input_files_gen:
            if not file_path.is_file():
                continue
            yield file_path

    def collect(self) -> None:
        """Collect files matching the glob pattern in the input directory. Once collected, the files can be accessed via the `input_files` property."""
        if self.has_collected():
            logger.info("Files have already been collected, skipping collection.")
            return None
        self._input_files = list(self.collect_stream())
        logger.info(f"DONE collecting files. Total {len(self._input_files)} files collected.")

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
        return list(self._metadata_list)


class MetadataCollector(IMetadataCollector, AsyncTaskPoolMixin):
    """
    Class responsible for collecting AssetMetadata from input files.
    It uses a FileCollector to gather the files and a Mapper to convert files to AssetMetadata.
    Currently, it supports GeoTIFF files and maps them to AssetMetadata using MapGeoTiffToAssetMetadata.
    """

    def __init__(
        self,
        file_collector: FileCollector,
        metadata_mapper: MapGeoTiffToAssetMetadata,
        max_outstanding_tasks: int = 10_000,
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

        # Initialize with callback that appends results as they complete
        self._max_outstanding_tasks = max_outstanding_tasks

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

    def _append_metadata(self, metadata: AssetMetadata) -> None:
        """Callback to append metadata as results complete."""
        self._metadata_list.append(metadata)

    def reset(self) -> None:
        super().reset()
        self._file_collector.reset()

    def get_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        if not self.file_collector.has_collected():
            for f in self.file_collector.collect_stream():
                yield f
        else:
            for f in self.file_collector.input_files:
                yield f

    def collect_stream(self) -> Generator[AssetMetadata, None, None]:
        """Collect metadata from input files, yielding results as they become available.

        This is a generator that yields AssetMetadata objects as soon as they are ready,
        allowing for streaming processing without waiting for all tasks to complete.

        Yields:
            AssetMetadata objects as they are collected from input files.
        """
        logger.debug("START streaming metadata collection from input files")

        # Use a queue to collect results as they complete
        result_queue: Queue[AssetMetadata] = Queue()

        def queue_callback(metadata: AssetMetadata):
            """Callback that puts results into the queue."""
            result_queue.put(metadata)

        self._init_async_task_pool(max_outstanding_tasks=self._max_outstanding_tasks, result_callback=queue_callback)

        submitted_count = 0
        completed_count = 0

        # Submit all tasks
        for file in self.get_input_files():
            self._submit_async_task(self.metadata_mapper.to_metadata, file)
            submitted_count += 1

            # Yield any completed results from the queue (non-blocking)
            while not result_queue.empty():
                completed_count += 1
                yield result_queue.get()

            # Log progress
            if submitted_count % self._max_outstanding_tasks == 0:
                pending = len(self._task_futures)
                logger.debug(
                    f"MetadataCollector: Submitted {submitted_count}, yielded {completed_count}, {pending} pending"
                )

        logger.info(f"All {submitted_count} files submitted. Collecting remaining results...")

        # Wait for all remaining tasks to complete
        self._wait_for_tasks(shutdown=True)

        # Yield all remaining results from the queue
        while not result_queue.empty():
            completed_count += 1
            yield result_queue.get()

        logger.debug(f"DONE streaming collection. Total {completed_count} metadata items yielded.")

    def collect(self) -> None:
        """Collect metadata from the input files using the metadata mapper.

        This method collects all metadata and stores it in self._metadata_list.
        For streaming results, use collect_stream() instead.
        """
        if self.has_collected():
            logger.warning("Metadata has already been collected, skipping collection.")
            return None
        self._metadata_list = list(self.collect_stream())
        logger.info(f"DONE collecting metadata. Total {len(self._metadata_list)} items collected.")
