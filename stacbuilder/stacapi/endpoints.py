import logging
from typing import List

from pystac import Collection, Item, ItemCollection

from yarl import URL

import requests
from requests.auth import AuthBase


_logger = logging.Logger(__name__)


class CollectionsEndpoint:
    def __init__(self, stac_api_url: URL, auth: AuthBase | None) -> None:
        self._stac_api_url = URL(stac_api_url)
        self._endpoint_url = self._stac_api_url / "collections"
        self._auth = auth or None

    @property
    def endpoint_url(self) -> URL:
        return self._endpoint_url

    def get_all(self) -> List[Collection]:
        response = requests.get(self.endpoint_url)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")
        return [Collection.from_dict(j) for j in data.get("collections", [])]

    def get(self, collection_id: str) -> Collection:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')

        response = requests.get(self.endpoint_url / str(collection_id))
        response.raise_for_status()
        if not response.status_code == 200:
            _logger.warning(
                f"Expecting HTTP status 200 but received {response.status_code!r}, response body:\n{response.text}"
            )
        return Collection.from_dict(response.json())

    def exists(self, collection_id: str) -> bool:
        response = requests.get(self.endpoint_url / str(collection_id))

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def create(self, collection: Collection):
        collection.validate()
        response = requests.post(self.endpoint_url, json=collection.to_dict())
        response.raise_for_status()
        if not response.status_code == 201:
            _logger.warning(
                f"Expecting HTTP status 201 but received {response.status_code!r}, response body:\n{response.text}"
            )
        return response.json()

    def update(self, collection: Collection):
        collection.validate()
        response = requests.put(self.endpoint_url, json=collection.to_dict())
        response.raise_for_status()
        # TODO what is the expected HTTP status code for updated?
        expected_status_codes = [200, 204]
        if response.status_code not in expected_status_codes:
            _logger.warning(
                f"Expecting HTTP status to be any of {expected_status_codes} "
                + f"but received {response.status_code!r}, response body:\n{response.text}"
            )
        return response.json()

    def delete(self, collection: Collection):
        collection.validate()
        response = requests.delete(self.endpoint_url, json=collection.to_dict())
        response.raise_for_status()
        return response.json()

    def create_or_update(self, collection: Collection):
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
        response = requests.get(self.get_items_url(collection_id))
        response.raise_for_status()
        data = response.json()
        # TODO: decide, Do we need to be strict about the expected HTTP status? For now this just logs a warning.
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return ItemCollection.from_dict(data)

    def get(self, collection_id: str, item_id: str) -> Item:
        response = requests.get(self.get_items_url_for_id(collection_id, item_id))
        response.raise_for_status()
        return Item.from_dict(response.json())

    def exists_by_id(self, collection_id: str, item_id: str) -> bool:
        response = requests.get(self.get_items_url_for_id(collection_id, item_id))

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def exists(self, item: Item) -> bool:
        return self.exists_by_id(item.collection_id, item.id)

    def create(self, item: Item):
        item.validate()
        response = requests.post(self.get_items_url(item.collection_id), item.to_dict())
        response.raise_for_status()
        if not response.status_code == 201:
            # TODO: decide, Do we need to be strict about the expectes HTTP status? For now this just logs a warning.
            _logger.warning(
                f"Expecting HTTP status 201 but received {response.status_code!r}, response body:\n{response.text}"
            )
        return response.json()

    def update(self, item: Item):
        item.validate()
        response = requests.put(self.get_items_url(item.collection_id), item.to_dict())
        response.raise_for_status()
        # TODO: should we log a warning when the HTTP status code is not one of the specific 2xx codes?
        return response.json()

    def create_or_update(self, item: Item):
        if self.exists(item):
            self.update(item)
        else:
            self.create(item)

    def delete_by_id(self, collection_id: str, item_id: str) -> bool:
        response = requests.delete(self.get_items_url_for_id(collection_id, item_id))
        response.raise_for_status()
        return response.json()

    def delete_item(self, item: Item) -> bool:
        return self.delete_by_id(item.collection_id, item.id)
