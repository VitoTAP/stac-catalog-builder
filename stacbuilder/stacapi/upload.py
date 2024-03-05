import logging
import inspect
import itertools
from pathlib import Path
from typing import Iterable

import pystac
from pystac import Collection, Item
from requests.auth import AuthBase
from yarl import URL


from stacbuilder.stacapi.auth import get_auth
from stacbuilder.stacapi.config import Settings
from stacbuilder.stacapi.endpoints import CollectionsEndpoint, ItemsEndpoint, RestApi


_logger = logging.Logger(__name__)


class Uploader:
    DEFAULT_BULK_SIZE = 20

    def __init__(
        self, collections_ep: CollectionsEndpoint, items_ep: ItemsEndpoint, bulk_size: int = DEFAULT_BULK_SIZE
    ) -> None:
        self._collections_endpoint = collections_ep
        self._items_endpoint = items_ep
        self._bulk_size = bulk_size

    @classmethod
    def from_settings(cls, settings: Settings) -> "Uploader":
        auth = get_auth(settings.auth)
        return cls.create_uploader(
            stac_api_url=settings.stac_api_url,
            auth=auth,
            collection_auth_info=settings.collection_auth_info,
            bulk_size=settings.bulk_size,
        )

    @staticmethod
    def create_uploader(
        stac_api_url: URL,
        auth: AuthBase | None,
        collection_auth_info: dict | None = None,
        bulk_size: int = DEFAULT_BULK_SIZE,
    ) -> "Uploader":
        rest_api = RestApi(base_url=stac_api_url, auth=auth)
        collections_endpoint = CollectionsEndpoint(
            rest_api=rest_api,
            collection_auth_info=collection_auth_info,
        )
        items_endpoint = ItemsEndpoint(rest_api)
        return Uploader(collections_ep=collections_endpoint, items_ep=items_endpoint, bulk_size=bulk_size)

    @property
    def bulk_size(self) -> int:
        return self._bulk_size

    @bulk_size.setter
    def bulk_size(self, value: int) -> int:
        self._bulk_size = int(value)

    def upload_collection(self, collection: Path | Collection) -> dict:
        if isinstance(collection, Path):
            collection = Collection.from_file(str(collection))
        elif not isinstance(collection, Collection):
            raise TypeError('Type of argument "collection" must either pathlib.Path or pystac.Collection')
        collection.validate()
        return self._collections_endpoint.create_or_update(collection)

    def upload_item(self, item) -> dict:
        if not isinstance(item, Item):
            raise TypeError('Type of argument "item" must either pathlib.Path or pystac.Item')
        item.validate()
        return self._items_endpoint.create_or_update(item)

    def upload_items_bulk(self, collection_id: str, items: Iterable[Item]) -> None:
        chunk = []
        chunk_start = 0
        chunk_end = 0

        for index, item in enumerate(items):
            self._prepare_item(item, collection_id)
            item.validate()
            chunk.append(item)

            if len(chunk) == self.bulk_size:
                chunk_end = index + 1
                chunk_start = chunk_end - len(chunk) + 1
                self._log_progress_message(f"Uploading items {chunk_start} to {chunk_end}")
                self._items_endpoint.ingest_bulk(chunk)
                chunk = []

        if chunk:
            chunk_end = index + 1
            chunk_start = chunk_end - len(chunk) + 1
            self._log_progress_message(f"Uploading items {chunk_start} to {chunk_end}")
            self._items_endpoint.ingest_bulk(chunk)

    def upload_collection_and_items(
        self, collection: Path | Collection, items: Path | list[Item], max_items: int = -1
    ) -> None:
        collection_out = self.upload_collection(collection)
        _logger.info(f"Uploaded collections, result={collection_out}")

        self.upload_items(collection, items, max_items)

    def upload_items(self, collection: Path | Collection, items: Path | list[Item], max_items: int = -1) -> None:
        if isinstance(collection, Path):
            collection = Collection.from_file(collection)

        items_out: list[Item] = items or []
        if not items:
            _logger.info(f"Using STAC items linked to the collection: {collection.id=}")
            items_out = collection.get_all_items()
        elif isinstance(items, Path):
            item_dir: Path = items
            _logger.info(f"Retrieving STAC items from JSON files in {item_dir=}")
            item_paths = list(item_dir.glob("*/*/*/*/*.json"))
            _logger.info(f"Number of STAC item files found: {len(item_paths)}")
            items_out = (Item.from_file(path) for path in item_paths)

        if max_items >= 0:
            _logger.info(f"User requested to limit the number of items to {max_items=}")
            items_out = itertools.islice(items_out, max_items)

        self.upload_items_bulk(collection.id, items_out)

    def _prepare_item(self, item: Item, collection_id: str):
        item.collection_id = collection_id

        if not item.get_links(pystac.RelType.COLLECTION):
            item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=item.collection_id))

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
        print(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
