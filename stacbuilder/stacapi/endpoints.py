import logging
from typing import Iterable

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
            + f"but received {response.status_code} - {response.reason}, request method={response.request.method}\n"
            + f"response body:\n{response.text}"
        )
        if raise_exc:
            raise Exception(message)
        else:
            _logger.warning(message)

    # Always raise errors on 4xx and 5xx status codes.
    response.raise_for_status()


class RestApi:
    """Helper class to execute the typical HTTP requests for a REST API

    Delegates the authentication in a consistent way with less code duplication.
    """

    def __init__(self, base_url: URL | str, auth: AuthBase | None = None) -> None:
        """Constructor

        :param base_url: the base URL of the API
            i.e. the part to which we concatentate URL paths.
            For example https//stac-api.my-organisation.com/api/

        :param auth: if present (= not None), this object takes care of authentication
            this is the same as the auth parameter in ` requests.request(method, url, **kwargs)`
            from the requests library.

            See also: https://requests.readthedocs.io/en/latest/api/#requests.request
        """
        self.base_url = URL(base_url)
        self.auth = auth or None

    def join_path(self, *url_path: list[str]) -> str:
        """Create a full URL path out of a list of strings.

        :param url_path:
            A string or a list of strings to join into a URL path.

        :return:
            The URL path, i.e. joining individual path parts joined with '/'
            To get the full URL (as a URL object) use join_url instead.
        """
        return "/".join(url_path)

    def join_url(self, url_path: str | list[str]) -> str:
        """Create a URL from the base_url and the url_path.

        :param url_path: same as in join_path
        :return: a URL object that represents the full URL.
        """
        return str(self.base_url / self.join_path(url_path))

    def get(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        """Execute an HTTP GET request.

        Authentication will be delegated to the AuthBase object self.auth, if it has a value,
        as per the requests library.
        If self.auth is None then no authentication done.

        :param url_path: path or path parts to build the full URL.
        :return: the HTTP response.
        """
        return requests.get(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def post(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        """Execute an HTTP POST request.

        Authentication will be delegated to the AuthBase object self.auth, if it has a value,
        as per the requests library.
        If self.auth is None then no authentication done.

        :param url_path: path or path parts to build the full URL.
        :return: the HTTP response.
        """
        return requests.post(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def put(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        """Execute an HTTP PUT request.

        Authentication will be delegated to the AuthBase object self.auth, if it has a value,
        as per the requests library.
        If self.auth is None then no authentication done.

        :param url_path: path or path parts to build the full URL.
        :return: the HTTP response.
        """
        return requests.put(self.join_url(url_path), auth=self.auth, *args, **kwargs)

    def delete(self, url_path: str | list[str], *args, **kwargs) -> requests.Response:
        """Execute an HTTP DELETE request.

        Authentication will be delegated to the AuthBase object self.auth, if it has a value,
        as per the requests library.
        If self.auth is None then no authentication done.

        :param url_path: path or path parts to build the full URL.
        :return: the HTTP response.
        """
        return requests.delete(self.join_url(url_path), auth=self.auth, *args, **kwargs)


class CollectionsEndpoint:
    def __init__(self, rest_api: RestApi, collection_auth_info: dict | None = None) -> None:
        """Constructor.

        Follows dependency injection so you have to provide the objects it needs
        (or mock implementation in a test) See parameters below.

        :param rest_api: the RestApi to delegate HTTP requests to.
        :param collection_auth_info: a dictionary that describe who can read or write the collection after creation.
            This dictionary is added to the collection's dictionary in the POST/POT request's body.
        """
        self._rest_api = rest_api
        self._collection_auth_info: dict | None = collection_auth_info or None

    @staticmethod
    def create_endpoint(
        stac_api_url: URL, auth: AuthBase | None, collection_auth_info: dict | None = None
    ) -> "CollectionsEndpoint":
        """Convenience method to create a CollectionsEndpoint object from basic information.

        This creates the dependencies for you, but that also means you can't pick another implementation here.
        If you need that (in a test) you should construct those objects yourself, and pass them directly to the constructor.
        """
        rest_api = RestApi(base_url=stac_api_url, auth=auth)
        return CollectionsEndpoint(
            rest_api=rest_api,
            collection_auth_info=collection_auth_info,
        )

    @property
    def stac_api_url(self) -> URL:
        """The base URL for the STAC API."""
        return self._rest_api.base_url

    def get_all(self) -> list[Collection]:
        """Get all collections.

        TODO: Implement paging: If there are many collections then the API will likely limit the number or collections returns, via paging.
        """
        response = self._rest_api.get("collections")

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return [Collection.from_dict(j) for j in data.get("collections", [])]

    def get(self, collection_id: str) -> Collection:
        """Get the collection with ID collection_id.

        :param collection_id: the collection ID to look for.
        :raises TypeError: when collection_id is not type str (string).
        :raises ValueError: when collection_id is an empty string.
        :raises HTTP: when the HTTP response status is 404 or any other error status.
        :return: a Collection object if it was found
        """
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
        """Query if a collection with ID collection_id exists.

        :param collection_id: the collection ID to look for.
        :raises TypeError: when collection_id is not type str (string).
        :raises ValueError: when collection_id is an empty string.
        :raises HTTP: when the HTTP response status any error status other than "404 Not found".
        :return: True if found, false if not fount.
        """
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
        """Create a new collection.

        :param collection: pystac.Collection object to create in the STAC API backend (or upload if you will)
        :raises TypeError: if collection is not a pystac.Collection.
        :return: dict that contains the JSON body of the HTTP response.
        """

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
        """Update an existing collection.

        :param collection: pystac.Collection object to update in the STAC API backend.
        :raises TypeError: if collection is not a pystac.Collection.
        :return: dict that contains the JSON body of the HTTP response.
        """

        if not isinstance(collection, Collection):
            raise TypeError(
                f'Argument "collection" must be of type pystac.Collection, but its type is {type(collection)=}'
            )

        collection.validate()
        data = self._add_authentication_section(collection)
        response = self._rest_api.put("collections", json=data)
        _check_response_status(response, _EXPECTED_STATUS_PUT)

        return response.json()

    def create_or_update(self, collection: Collection) -> dict:
        """'Upsert': Create the collection if it does not exist, or update it if it exists.

        :param collection: the collection to create/update
        :return: dict that contains the JSON body of the HTTP response.
        """

        # TODO: decide: Another strategy could be to handle HTTP 409 conflict and the fall back to a self.update / PUT request
        if self.exists(collection.id):
            return self.update(collection)
        else:
            return self.create(collection)

    def delete(self, collection: Collection):
        """Delete this collection.

        :param collection: pystac.Collection object to delete from the STAC API backend.
        :raises TypeError: if collection is not a pystac.Collection.
        :return: dict that contains the JSON body of the HTTP response.
        """
        return self.delete_by_id(collection.id)

    def delete_by_id(self, collection_id: str):
        """Delete the collection that has the specified ID.

        :param collection_id: the collection ID to look for.
        :raises TypeError: when collection_id is not a string.
        :raises ValueError: when collection_id is an empty string.
        :return: dict that contains the JSON body of the HTTP response.
        """
        if not isinstance(collection_id, str):
            raise TypeError(f'Argument "collection_id" must be of type str, but its type is {type(collection_id)=}')

        if collection_id == "":
            raise ValueError(
                f'Argument "collection_id" must have a value; it can not be the empty string. {collection_id=!r}'
            )

        response = self._rest_api.delete(f"collections/{collection_id}")
        _check_response_status(response, _EXPECTED_STATUS_DELETE)


    def _add_authentication_section(self, collection: Collection) -> dict:
        coll_dict = collection.to_dict()
        if self._collection_auth_info:
            coll_dict.update(self._collection_auth_info)

        return coll_dict


class ItemsEndpoint:
    def __init__(self, rest_api: RestApi) -> None:
        self._rest_api: RestApi = rest_api

    @staticmethod
    def create_endpoint(stac_api_url: URL, auth: AuthBase | None) -> "ItemsEndpoint":
        return ItemsEndpoint(rest_api=RestApi(base_url=stac_api_url, auth=auth))

    @property
    def stac_api_url(self) -> URL:
        return self._rest_api.base_url

    def get_items_url(self, collection_id) -> URL:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" must have a value of type str. {collection_id=!r}')
        return f"collections/{collection_id}/items"

    def get_items_url_for_id(self, collection_id: str, item_id: str) -> URL:
        if not collection_id:
            raise ValueError(f'Argument "collection_id" miust have a value of type str. {collection_id=!r}')
        if not item_id:
            raise ValueError(f'Argument "item_id" must have a value of type str. {item_id=!r}')
        return f"collections/{collection_id}/items/{item_id}"

    def get_items_url_for_item(self, item: Item) -> URL:
        if not item:
            raise ValueError(f'Argument "item" must be a pystac.Item instance. {type(item)=}, {item=!r}')
        return self.get_items_url_for_id(item.collection_id, item.id)

    def get_all(self, collection_id) -> ItemCollection:
        response = self._rest_api.get(self.get_items_url(collection_id))

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return ItemCollection.from_dict(data)

    def get(self, collection_id: str, item_id: str) -> Item:
        response = self._rest_api.get(self.get_items_url_for_id(collection_id, item_id))

        _check_response_status(response, _EXPECTED_STATUS_GET)
        data = response.json()
        if not isinstance(data, dict):
            raise Exception(f"Expected a dict in the JSON body but received type {type(data)}, value={data!r}")

        return Item.from_dict(data)

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
        response = self._rest_api.get(self.get_items_url_for_id(collection_id, item_id))

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

        response = self._rest_api.post(self.get_items_url(item.collection_id), json=item.to_dict())
        _logger.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")
        print(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")

        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def update(self, item: Item) -> dict:
        item.validate()

        response = self._rest_api.put(self.get_items_url_for_id(item.collection_id, item.id), json=item.to_dict())
        _logger.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")
        print(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")

        _check_response_status(response, _EXPECTED_STATUS_PUT)
        return response.json()

    def ingest_bulk(self, items: Iterable[Item], max_retries=5, retries=0) -> dict:
        collection_id = items[0].collection_id
        if not all(i.collection_id == collection_id for i in items):
            raise Exception("All collection IDs should be identical for bulk ingests")

        url_path = f"collections/{collection_id}/bulk_items"
        data = {"method": "upsert", "items": {item.id: item.to_dict() for item in items}}
        try:
            response = self._rest_api.post(url_path, json=data)
            _logger.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")
            _check_response_status(response, _EXPECTED_STATUS_POST)
        except requests.HTTPError as e:
            _logger.warning(f"ingest_bulk failed: retries={retries}, max_retries={max_retries}")
            if retries < max_retries:
                return self.ingest_bulk(items, max_retries, retries + 1)
            else:
                _logger.error(f"ingest_bulk failed after {max_retries} retries")
                raise e
        return response.json()

    def create_or_update(self, item: Item) -> dict:
        if self.exists(item):
            return self.update(item)
        else:
            return self.create(item)

    def delete_by_id(self, collection_id: str, item_id: str):
        if not collection_id:
            raise ValueError(
                "collection_id must have a non-empty str value."
                + f"Actual type and value: {type(collection_id)=}, {collection_id=!r}"
            )
        if not item_id:
            raise InvalidOperation(
                f"item_id must have a non-empty str value. Actual type and value: {type(item_id)=}, {item_id=!r}"
            )

        response = self._rest_api.delete(self.get_items_url_for_id(collection_id, item_id))
        _logger.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")
        print(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")

        _check_response_status(response, _EXPECTED_STATUS_DELETE)


    def delete_item(self, item: Item):
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
