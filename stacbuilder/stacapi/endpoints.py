import logging
from pathlib import Path
from typing import List

from pystac import Collection, Item, ItemCollection

from yarl import URL

import requests
import requests.status_codes
from requests.auth import AuthBase


from stacbuilder.exceptions import InvalidOperation


_logger = logging.Logger(__name__)


_EXPECTED_STATUS_GET = [requests.status_codes.codes.ok]
_EXPECTED_STATUS_POST = [
    requests.status_codes.codes.ok,
    requests.status_codes.codes.created,
    requests.status_codes.codes.accepted,
]
_EXPECTED_STATUS_PUT = [
    requests.status_codes.codes.ok,
    requests.status_codes.codes.created,
    requests.status_codes.codes.accepted,
    requests.status_codes.codes.no_content,
]
_EXPECTED_STATUS_DELETE = _EXPECTED_STATUS_PUT


def _check_response_status(response: requests.Response, expected_status_codes: list[int]):
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

        self._collection_auth_info: dict | None = None

    @property
    def stac_api_url(self) -> URL:
        return self._stac_api_url

    @property
    def collections_url(self) -> URL:
        return self._collections_url

    def get_all(self) -> List[Collection]:
        response = requests.get(str(self.collections_url), auth=self._auth)

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")
        return [Collection.from_dict(j) for j in data.get("collections", [])]

    def get(self, collection_id: str) -> Collection:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')

        url_str = str(self.collections_url / str(collection_id))
        response = requests.get(url_str, auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return Collection.from_dict(response.json())

    def exists(self, collection_id: str) -> bool:
        url_str = str(self.collections_url / str(collection_id))
        response = requests.get(url_str, auth=self._auth)

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == requests.status_codes.codes.not_found:
            return False
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return True

    def create(self, collection: Collection) -> dict:
        collection.validate()
        response = requests.post(str(self.collections_url), json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def update(self, collection: Collection) -> dict:
        collection.validate()
        response = requests.put(str(self.collections_url), json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_PUT)
        return response.json()

    def delete(self, collection: Collection) -> dict:
        collection.validate()
        response = requests.delete(str(self.collections_url), json=collection.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_DELETE)
        return response.json()

    def create_or_update(self, collection: Collection) -> dict:
        # TODO: decide: Another strategy could be to handle HTTP 409 conflict and the fall back to a self.update / PUT request
        if self.exists(collection.id):
            return self.update(collection)
        else:
            return self.create(collection)

    def _add_authentication_section(self, collection) -> dict:
        coll_dict = collection.to_dict()
        if self._collection_auth_info:
            coll_dict.update(self._collection_auth_info)
        return coll_dict


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
        url_str = str(self.get_items_url(collection_id))
        response = requests.get(url_str, auth=self._auth)
        response.raise_for_status()
        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return ItemCollection.from_dict(data)

    def get(self, collection_id: str, item_id: str) -> Item:
        url_str = str(self.get_items_url_for_id(collection_id, item_id))
        response = requests.get(url_str, auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return Item.from_dict(response.json())

    def exists_by_id(self, collection_id: str, item_id: str) -> bool:
        if not collection_id:
            raise ValueError(
                "collection_id must have a non-empty str value."
                + f"Actual type and value: {type(collection_id)=}, {collection_id=!r}"
            )
        if not item_id:
            raise InvalidOperation(
                f"item_id must have a non-empty str value. Actual type and value: {type(item_id)=}, {item_id=!r}"
            )
        url_str = str(self.get_items_url_for_id(collection_id, item_id))
        response = requests.get(url_str, auth=self._auth)

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == 404:
            return False
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return True

    def exists(self, item: Item) -> bool:
        if not item.collection_id:
            raise InvalidOperation(
                "Can not check if item exists in backend: item.collection_id must be a non-empty str value."
                + f"Actual type and value: {type(item.collection_id)=}, {item.collection_id=!r}"
            )
        if not item.collection_id:
            raise InvalidOperation(
                "Can not check if item exists in backend: item.id must have a value (a non-empty str value)."
                + f"Actual type and value: {type(item.id)=}, {item.id=!r}"
            )
        return self.exists_by_id(item.collection_id, item.id)

    def create(self, item: Item) -> dict:
        item.validate()
        url_str = str(self.get_items_url(item.collection_id))
        response = requests.post(url_str, json=item.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def update(self, item: Item) -> dict:
        item.validate()
        url_str = str(self.get_items_url_for_id(item.collection_id, item.id))
        response = requests.put(url_str, json=item.to_dict(), auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_PUT)
        return response.json()

    def create_or_update(self, item: Item) -> dict:
        if self.exists(item):
            return self.update(item)
        else:
            return self.create(item)

    def delete_by_id(self, collection_id: str, item_id: str) -> dict:
        if not collection_id:
            raise ValueError(
                "collection_id must have a non-empty str value."
                + f"Actual type and value: {type(collection_id)=}, {collection_id=!r}"
            )
        if not item_id:
            raise InvalidOperation(
                f"item_id must have a non-empty str value. Actual type and value: {type(item_id)=}, {item_id=!r}"
            )
        url_str = str(self.get_items_url_for_id(collection_id, item_id))
        response = requests.delete(url_str, auth=self._auth)
        _check_response_status(response, _EXPECTED_STATUS_DELETE)
        return response.json()

    def delete_item(self, item: Item) -> dict:
        if not item.collection_id:
            raise InvalidOperation(
                "Can not delete item: item.collection_id must be a non-empty str value."
                + f"Actual type and value: {type(item.collection_id)=}, {item.collection_id=!r}"
            )
        if not item.collection_id:
            raise InvalidOperation(
                "Can not delete item: item.id must have a value (a non-empty str value)."
                + f"Actual type and value: {type(item.id)=}, {item.id=!r}"
            )
        return self.delete_by_id(item.collection_id, item.id)


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


__all__ = ["CollectionsEndpoint", "ItemsEndpoint", "Uploader"]
