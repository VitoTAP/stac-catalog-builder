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
    def __init__(self, stac_api_url: URL, auth: AuthBase | None) -> None:
        self._stac_api_url = URL(stac_api_url)
        self._auth = auth or None
        self._collections_endpoint = CollectionsEndpoint(stac_api_url=self._stac_api_url, auth=self._auth)
        self._items_endpoint = ItemsEndpoint(stac_api_url=self._stac_api_url, auth=self._auth)

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

    @staticmethod
    def from_settings(settings: Settings) -> "Uploader":
        auth = get_auth(settings.auth)
        return Uploader(stac_api_url=settings.stac_api_url, auth=auth)
