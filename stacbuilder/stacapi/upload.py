import logging
from pathlib import Path

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

    @staticmethod
    def from_settings(settings: Settings) -> "Uploader":
        auth = get_auth(settings.auth)
        return Uploader(
            stac_api_url=settings.stac_api_url, auth=auth, collection_auth_info=settings.collection_auth_info
        )

    @classmethod
    def setup(cls, stac_api_url: URL, auth: AuthBase | None, collection_auth_info: dict | None = None) -> "Uploader":
        collections_endpoint = CollectionsEndpoint(
            stac_api_url=stac_api_url,
            auth=auth,
            collection_auth_info=collection_auth_info,
        )
        items_endpoint = ItemsEndpoint(stac_api_url=stac_api_url, auth=auth)
        return Uploader(collections_ep=collections_endpoint, items_ep=items_endpoint)

    def upload_collection(self, collection: Path | Collection) -> Collection:
        if isinstance(collection, Path):
            collection = Collection.from_file(str(collection))
        elif not isinstance(collection, Collection):
            raise TypeError('Type of argument "collection" must either pathlib.Path or pystac.Collection')
        collection.validate()
        self._collections_endpoint.create_or_update(collection)
        return collection

    def upload_item(self, item: Path | Item) -> Item:
        if isinstance(item, Path):
            item = Item.from_file(str(item))
        elif not isinstance(item, Item):
            raise TypeError('Type of argument "item" must either pathlib.Path or pystac.Item')
        item.validate()
        self._items_endpoint.create_or_update(item)
        return item

    def upload_collection_and_items(self, collection: Path | Collection):
        collection_out = self.upload_collection(collection)
        for item in collection_out.get_all_items():
            self.upload_item(item)
