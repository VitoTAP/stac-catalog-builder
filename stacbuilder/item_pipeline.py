from pathlib import Path
from typing import Generator, Optional, Callable, Dict, Hashable
from pystac import Item, Collection
import inspect
import logging

from stacbuilder import CollectionConfig
from stacbuilder.builder import CollectionBuilder
from stacbuilder.collector import IDataCollector

_logger = logging.getLogger(__name__)

class ItemMetadataPipeline:
    """
    Creates a STAC Collection and Items from AssetMetadata. This class is the main entry point for the STAC catalog builder.

    The AssetMetadata is collected by the IMetadataCollector implementation, which is passed to the constructor.
    """

    def __init__(
        self,
        metadata_collector: IDataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        link_items: Optional[bool] = True,
        item_postprocessor: Optional[Callable] = None,
    ) -> None:
        if output_dir and not isinstance(output_dir, Path):
            raise TypeError(f"Argument output_dir (if not None) should be of type Path, {type(output_dir)=}")

        #if collection_config is None:
        #    raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if metadata_collector is None:
            raise ValueError(
                'Argument "metadata_collector" can not be None, must be a IDataCollector implementation.'
            )
        # Settings: these are just data, not components we delegate work to.
        self._collection_dir: Path = output_dir
        self._link_items = bool(link_items)
        self.collection_config: CollectionConfig = collection_config

        # Components / dependencies that must be provided
        self.metadata_collector: IDataCollector = metadata_collector

        # Components / dependencies that we set up internally

        self._func_find_item_group: Optional[Callable[[Item], str]] = None

        self.collection_builder: CollectionBuilder = CollectionBuilder(
            collection_config=self.collection_config,
            output_dir=self._collection_dir,
            link_items=self._link_items,
        )

        self.item_postprocessor: Optional[Callable] = item_postprocessor

        # results
        self.collection: Optional[Collection] = None
        self.collection_groups: Dict[Hashable, Collection] = {}

    def collect_stac_items(self):

        self.metadata_collector.collect()
        items = self.metadata_collector.input_items
        if self.item_postprocessor:
            items = [self.item_postprocessor(item) for item in items]
        return items

    def build_collection(
        self,
    ):
        """Build the entire STAC collection."""
        self._log_progress_message("START: build_collection")

        assert self._collection_dir, "Collection directory must be set before building the collection."

        #self.reset()

        # This calls the item builder to create STAC Items from AssetMetadata.
        item_generator: Generator[Item] = self.collect_stac_items()

        # This passes the STAC Items to the collection builder to create a STAC Collection.
        self.collection = self.collection_builder.build_collection_from_items(item_generator, save_collection=True)

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")