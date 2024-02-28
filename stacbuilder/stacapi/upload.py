import logging
from itertools import islice
from pathlib import Path
from typing import Iterable

import pystac
from pystac import Collection, Item
from requests.auth import AuthBase
from yarl import URL


from stacbuilder.stacapi.auth import get_auth
from stacbuilder.stacapi.config import Settings
from stacbuilder.stacapi.endpoints import CollectionsEndpoint, ItemsEndpoint


_logger = logging.Logger(__name__)


class Uploader:
    def __init__(self, collections_ep: CollectionsEndpoint, items_ep: ItemsEndpoint) -> None:
        self._collections_endpoint = collections_ep
        self._items_endpoint = items_ep

    @classmethod
    def from_settings(cls, settings: Settings) -> "Uploader":
        auth = get_auth(settings.auth)
        return cls.setup(
            stac_api_url=settings.stac_api_url, auth=auth, collection_auth_info=settings.collection_auth_info
        )

    @staticmethod
    def setup(stac_api_url: URL, auth: AuthBase | None, collection_auth_info: dict | None = None) -> "Uploader":
        collections_endpoint = CollectionsEndpoint(
            stac_api_url=stac_api_url,
            auth=auth,
            collection_auth_info=collection_auth_info,
        )
        items_endpoint = ItemsEndpoint(stac_api_url=stac_api_url, auth=auth)
        return Uploader(collections_ep=collections_endpoint, items_ep=items_endpoint)

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
        breakpoint()
        for item in items:
            self._prepare_item(item, collection_id)
            item.validate()
            self.upload_item(item)

    def upload_collection_and_items(
        self, collection: Path | Collection, items: Path | list[Item], max_items: int = -1
    ) -> None:
        breakpoint()
        collection_out = self.upload_collection(collection)

        breakpoint()
        self.upload_items(collection_out, items, max_items)

    def upload_items(self, collection: Path | Collection, items: Path | list[Item], max_items: int = -1) -> None:
        if isinstance(collection, Path):
            collection = Collection.from_file(collection)

        breakpoint()
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

        breakpoint()
        if max_items >= 0:
            _logger.info(f"User requested to limit the number of items to {max_items=}")
            items_out = islice(items_out, max_items)

        self.upload_items_bulk(collection.id, items_out)

    def _prepare_item(self, item: Item, collection_id: str):
        breakpoint()
        item.collection_id = collection_id

        if not item.get_links(pystac.RelType.COLLECTION):
            item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=item.collection_id))
