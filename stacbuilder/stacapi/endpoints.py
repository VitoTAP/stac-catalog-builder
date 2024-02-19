import logging
from pathlib import Path
from typing import List

from pystac import Collection, Item, ItemCollection

from yarl import URL

import requests
from requests.auth import AuthBase


_logger = logging.Logger(__name__)


_EXPECTED_STATUS_GET = [200]
_EXPECTED_STATUS_POST = [201, 202]
_EXPECTED_STATUS_PUT = [200, 202, 204]
_EXPECTED_STATUS_DELETE = _EXPECTED_STATUS_PUT


def _check_response_status(response, expected_status_codes=[200, 202, 204]):
    response.raise_for_status()
    if response.status_code not in expected_status_codes:
        _logger.warning(
            f"Expecting HTTP status to be any of {expected_status_codes} "
            + f"but received {response.status_code!r}, response body:\n{response.text}"
        )


class CollectionsEndpoint:
    def __init__(self, stac_api_url: URL, auth: AuthBase | None) -> None:
        self._stac_api_url = URL(stac_api_url)
        self._collections_url = self._stac_api_url / "collections"
        self._auth = auth or None

    @property
    def stac_api_url(self) -> URL:
        return self._stac_api_url

    @property
    def collections_url(self) -> URL:
        return self._collections_url

    def get_all(self) -> List[Collection]:
        response = requests.get(self.collections_url, auth=self._auth)

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")
        return [Collection.from_dict(j) for j in data.get("collections", [])]

    def get(self, collection_id: str) -> Collection:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')

        response = requests.get(self.collections_url / str(collection_id), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return Collection.from_dict(response.json())

    def exists(self, collection_id: str) -> bool:
        response = requests.get(self.collections_url / str(collection_id), auth=self._auth)

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == 404:
            return False
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return True

    def create(self, collection: Collection):
        collection.validate()
        response = requests.post(self.collections_url, json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def update(self, collection: Collection):
        collection.validate()
        response = requests.put(self.collections_url, json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_PUT)
        return response.json()

    def delete(self, collection: Collection):
        collection.validate()
        response = requests.delete(self.collections_url, json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_DELETE)
        return response.json()

    def create_or_update(self, collection: Collection):
        # TODO: decide: Another strategy could be to handle HTTP 409 conflict and the fall back to a self.update / PUT request
        if self.exists(collection.id):
            self.update(collection)
        else:
            self.create(collection)


class ItemsEndpoint:
    def __init__(self, stac_api_url: URL, auth: AuthBase | None) -> None:
        self._stac_api_url = URL(stac_api_url)
        self._auth = auth or None

    @property
    def stac_api_url(self) -> URL:
        return self._stac_api_url

    def get_items_url(self, collection_id) -> URL:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')
        return self._stac_api_url / "collections" / str(collection_id) / "items"

    def get_items_url_for_id(self, collection_id, item_id) -> URL:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')
        if not item_id:
            raise ValueError(f'Argument "item_id" must have a value of type str. {item_id=!r}')
        return self._stac_api_url / "collections" / str(collection_id) / "items" / str(item_id)

    def get_items_url_for_item(self, item: Item) -> URL:
        if not item:
            raise ValueError(f'Argument "item" must be a pystac.Item instance. {type(item)=}, {item=!r}')
        return self.get_items_url_for_id(item.collection_id, item.id)

    def get_all(self, collection_id) -> ItemCollection:
        url = self.get_items_url(collection_id)
        response = requests.get(url, auth=self._auth)
        response.raise_for_status()
        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return ItemCollection.from_dict(data)

    def get(self, collection_id: str, item_id: str) -> Item:
        url = self.get_items_url_for_id(collection_id, item_id)
        response = requests.get(url, auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return Item.from_dict(response.json())

    def exists_by_id(self, collection_id: str, item_id: str) -> bool:
        url = self.get_items_url_for_id(collection_id, item_id)
        response = requests.get(url, auth=self._auth)

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == 404:
            return False
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return True

    def exists(self, item: Item) -> bool:
        return self.exists_by_id(item.collection_id, item.id)

    def create(self, item: Item):
        item.validate()
        url = self.get_items_url(item.collection_id)
        response = requests.post(url, json=item.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def update(self, item: Item):
        item.validate()
        url = self.get_items_url_for_id(item.collection_id, item.id)
        response = requests.put(url, json=item.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_PUT)
        return response.json()

    def create_or_update(self, item: Item):
        if self.exists(item):
            self.update(item)
        else:
            self.create(item)

    def delete_by_id(self, collection_id: str, item_id: str) -> bool:
        url = self.get_items_url_for_id(collection_id, item_id)
        response = requests.delete(url, auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_DELETE)
        return response.json()

    def delete_item(self, item: Item) -> bool:
        return self.delete_by_id(item.collection_id, item.id)


class Ingestor:
    def __init__(self, stac_api_url: URL, auth: AuthBase | None) -> None:
        self._stac_api_url = URL(stac_api_url)
        self._auth = auth or None
        self._collections_endpoint = CollectionsEndpoint(stac_api_url=self._stac_api_url, auth=self._auth)
        self._items_endpoint = ItemsEndpoint(stac_api_url=self._stac_api_url, auth=self._auth)

    def ingest_collection(self, path_collection: Path):
        collection = Collection.from_file(path_collection)
        collection.validate()
        self._collections_endpoint.create_or_update(collection)

    def ingest_item(self, path_item: Path):
        item = Item.from_file(path_item)
        item.validate()
        self._items_endpoint.create_or_update(item)


__all__ = ["CollectionsEndpoint", "ItemsEndpoint", "Ingestor"]
