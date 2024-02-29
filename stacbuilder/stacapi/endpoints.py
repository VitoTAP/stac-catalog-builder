import logging
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


def _check_response_status(response: requests.Response, expected_status_codes: list[int], raise_exc: bool = False):
    if response.status_code not in expected_status_codes:
        message = (
            f"Expecting HTTP status to be any of {expected_status_codes} "
            + f"but received {response.status_code!r}, response body:\n{response.text}"
        )
        if raise_exc:
            raise Exception(message)
        else:
            _logger.warning(message)

    # Always raise errors on 4xx and 5xx status codes.
    response.raise_for_status()


class RestApi:
    def __init__(self, base_url: URL | str, auth: AuthBase | None = None) -> None:
        self.base_url = URL(base_url)
        self.auth = auth or None

    def join_path(self, *url_path: list[str]) -> str:
        return "/".join(url_path)
        # if isinstance(url_path, list):
        #     return "/".join(url_path)
        # return url_path

    def join_url(self, url_path: str | list[str]) -> str:
        return str(self.base_url / self.join_path(url_path))

    def get(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        return requests.get(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def post(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        return requests.post(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def put(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        return requests.put(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def delete(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        return requests.delete(self.join_url(url_path), auth=self.auth, *args, **kwargs)


class CollectionsEndpoint:
    def __init__(self, rest_api: RestApi, collection_auth_info: dict | None = None) -> None:
        self._rest_api = rest_api
        # self._collections_path = "collections"
        self._collection_auth_info: dict | None = collection_auth_info or None

    @staticmethod
    def create_endpoint(
        stac_api_url: URL, auth: AuthBase | None, collection_auth_info: dict | None = None
    ) -> "CollectionsEndpoint":
        rest_api = RestApi(base_url=stac_api_url, auth=auth)
        return CollectionsEndpoint(
            rest_api=rest_api,
            collection_auth_info=collection_auth_info,
        )

    @property
    def stac_api_url(self) -> URL:
        return self._rest_api.base

    @property
    # def collections_path(self) -> str:
    #     return self._collections_path

    # def _join_path(self, *url_path: list[str]) -> str:
    #     return self._rest_api.join_path(*url_path)

    # def get_collections_path(self, collection_id: str | None = None) -> str:
    #     if not collection_id:
    #         return self._rest_api.join_path("collections")
    #     return self._rest_api.join_path("collections",  str(collection_id))

    # def get_collections_url(self, collection_id: str | None) -> URL:
    #     if not collection_id:
    #         return self._rest_api.join_url("collections")
    #     return self._rest_api.join_url("collections",  str(collection_id))

    def get_all(self) -> List[Collection]:
        response = self._rest_api.get("collections")

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return [Collection.from_dict(j) for j in data.get("collections", [])]

    def get(self, collection_id: str) -> Collection:
        if not isinstance(collection_id, str):
            raise TypeError(f'Argument "collection_id" must be of type str, but its type is {type(collection_id)=}')

        if collection_id == "":
            raise ValueError(
                f'Argument "collection_id" must have a value; it can not be the empty string. {collection_id=!r}'
            )

        response = self._rest_api.get(f"collections/{collection_id}")
        _check_response_status(response, _EXPECTED_STATUS_GET)

        return Collection.from_dict(response.json())

    def exists(self, collection_id: str) -> bool:
        if not isinstance(collection_id, str):
            raise TypeError(f'Argument "collection_id" must be of type str, but its type is {type(collection_id)=}')

        if collection_id == "":
            raise ValueError(
                f'Argument "collection_id" must have a value; it can not be the empty string. {collection_id=!r}'
            )

        response = self._rest_api.get(f"collections/{collection_id}")

        # We do expect HTTP 404 when it doesn't exist.
        # Any other error status means there is an actual problem.
        if response.status_code == requests.status_codes.codes.not_found:
            return False
        _check_response_status(response, _EXPECTED_STATUS_GET)
        return True

    def create(self, collection: Collection) -> dict:
        if not isinstance(collection, Collection):
            raise TypeError(
                f'Argument "collection" must be of type pystac.Collection, but its type is {type(collection)=}'
            )

        collection.validate()
        data = self._add_authentication_section(collection)
        response = self._rest_api.post("collections", json=data)
        _check_response_status(response, _EXPECTED_STATUS_POST)

        return response.json()

    def update(self, collection: Collection) -> dict:
        if not isinstance(collection, Collection):
            raise TypeError(
                f'Argument "collection" must be of type pystac.Collection, but its type is {type(collection)=}'
            )

        collection.validate()
        data = self._add_authentication_section(collection)
        response = self._rest_api.put(f"collections/{collection.id}", json=data)
        _check_response_status(response, _EXPECTED_STATUS_PUT)

        return response.json()

    def delete(self, collection: Collection) -> dict:
        return self.delete_by_id(collection.id)

    def delete_by_id(self, collection_id: str) -> dict:
        if not isinstance(collection_id, str):
            raise TypeError(f'Argument "collection_id" must be of type str, but its type is {type(collection_id)=}')

        if collection_id == "":
            raise ValueError(
                f'Argument "collection_id" must have a value; it can not be the empty string. {collection_id=!r}'
            )

        response = self._rest_api.delete(f"collections/{collection_id}")
        _check_response_status(response, _EXPECTED_STATUS_DELETE)
        return response.json()

    def create_or_update(self, collection: Collection) -> dict:
        # TODO: decide: Another strategy could be to handle HTTP 409 conflict and the fall back to a self.update / PUT request
        if self.exists(collection.id):
            return self.update(collection)
        else:
            return self.create(collection)

    def _add_authentication_section(self, collection: Collection) -> dict:
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


__all__ = ["CollectionsEndpoint", "ItemsEndpoint"]
