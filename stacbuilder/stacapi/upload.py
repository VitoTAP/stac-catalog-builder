import inspect
import itertools
from pathlib import Path
from typing import Iterable

import pystac
from loguru import logger
from pystac import Collection, Item
from requests.auth import AuthBase
from yarl import URL

from stacbuilder.async_utils import AsyncTaskPoolMixin
from stacbuilder.stacapi.auth import get_auth
from stacbuilder.stacapi.config import Settings
from stacbuilder.stacapi.endpoints import CollectionsEndpoint, ItemsEndpoint, RestApi


class Uploader(AsyncTaskPoolMixin):
    DEFAULT_BULK_SIZE = 20

    def __init__(
        self, collections_ep: CollectionsEndpoint, items_ep: ItemsEndpoint, bulk_size: int = DEFAULT_BULK_SIZE
    ) -> None:
        self._init_async_task_pool(max_outstanding_tasks=100)
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

    def delete_collection(self, id: str):
        return self._collections_endpoint.delete_by_id(id)

    def upload_collection(self, collection: Path | Collection) -> dict:
        if isinstance(collection, Path):
            collection = Collection.from_file(collection)
        elif not isinstance(collection, Collection):
            raise TypeError('Type of argument "collection" must either pathlib.Path or pystac.Collection')
        collection.validate()
        return self._collections_endpoint.create_or_update(collection)

    def upload_item(self, item) -> dict:
        if not isinstance(item, Item):
            raise TypeError('Type of argument "item" must either pathlib.Path or pystac.Item')
        item.validate()
        return self._items_endpoint.create_or_update(item)

    @staticmethod
    def chunk_items(items: Iterable[Item], chunk_size: int) -> Iterable[list[Item]]:
        items_iter = iter(items)
        chunk = list(itertools.islice(items_iter, chunk_size))
        while chunk:
            yield chunk
            chunk = list(itertools.islice(items_iter, chunk_size))

    def upload_items_bulk(self, collection_id: str, items: Iterable[Item]) -> None:
        for index, chunk in enumerate(Uploader.chunk_items(items, self.bulk_size)):
            for item in chunk:
                self._prepare_item(item, collection_id)
            start_index = index * self.bulk_size
            self._log_progress_message(f"Uploading bulk from item {start_index} to {start_index + len(chunk)}")
            self._submit_async_task(self._items_endpoint.ingest_bulk, chunk.copy())

        # Wait for all uploads to complete
        try:
            self._wait_for_tasks()
            self._log_progress_message("All items uploaded")
        except RuntimeError as e:
            self._log_progress_message(f"Some items failed to upload: {e}")
            raise e

    def upload_collection_and_items(
        self,
        collection: Path | Collection,
        items: Path | list[Item],
        limit: int = -1,
        offset: int = -1,
    ) -> None:
        collection_out = self.upload_collection(collection)
        logger.info(f"Uploaded collections, result={collection_out}")

        self.upload_items(collection, items, limit=limit, offset=offset)

    def upload_items(
        self,
        collection: Path | Collection,
        items: Path | list[Item],
        limit: int = -1,
        offset: int = -1,
        item_glob: str = "*/*/*/*/*.json",
    ) -> None:
        if isinstance(collection, Path):
            collection = Collection.from_file(collection)

        items_out: list[Item] = items or []
        if not items:
            logger.info(f"Using STAC items linked to the collection: {collection.id=}")
            items_out = collection.get_all_items()
        elif isinstance(items, Path):
            item_dir: Path = items
            logger.info(f"Retrieving STAC items from JSON files in {item_dir=}")
            item_paths = list(item_dir.glob(item_glob))
            logger.info(f"Number of STAC item files found: {len(item_paths)}")
            items_out = (Item.from_file(path) for path in item_paths)

        start = None
        stop = None
        if offset > 0:
            start = offset
            logger.info(f"User requested to start item upload at offset {offset=}")

        if limit > 0:
            logger.info(f"User requested to limit the number of items to {limit=}")
            if offset > 0:
                stop = offset + limit
            else:
                stop = limit

        self._log_progress_message(f"START upload of items from {start=} to {stop=}. ({offset=}, {limit=})")

        items_out = itertools.islice(items_out, start, stop)
        self.upload_items_bulk(collection.id, items_out)

        self._log_progress_message(f"DONE upload of items from {start=} to {stop=}. ({offset=}, {limit=})")

    def _prepare_item(self, item: Item, collection_id: str):
        item.collection_id = collection_id

        if not item.get_links(pystac.RelType.COLLECTION):
            item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=item.collection_id))

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
