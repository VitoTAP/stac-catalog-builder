import inspect
import itertools
from pathlib import Path
from typing import Iterable

import pystac
from loguru import logger
from pystac import Collection, Extent, Item, SpatialExtent, TemporalExtent
from requests.auth import AuthBase
from upath import UPath
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

    def merge_and_upload_collection(self, new_collection: Path | Collection) -> Collection:
        """Upload a collection to the STAC API, merging with the existing one if already present.

        The ``new_collection`` is treated as the authoritative source of metadata (title,
        description, keywords, providers, stac_extensions).  If a collection with the same
        ID already exists on the API its spatial and temporal extents are loaded and merged
        with the new collection's extents so that the published collection always covers all
        previously-uploaded items as well as the new ones.

        :param new_collection: The freshly-built collection to upload.
            Can be a :class:`pathlib.Path` pointing to a ``collection.json`` file,
            or a :class:`pystac.Collection` object.
        :raises TypeError: when ``new_collection`` is neither a Path nor a Collection.
        :return: The collection as it was uploaded to the STAC API.
        """
        if isinstance(new_collection, Path):
            new_collection = Collection.from_file(new_collection)
        elif not isinstance(new_collection, Collection):
            raise TypeError('Type of argument "new_collection" must be a pathlib.Path or pystac.Collection')

        if self._collections_endpoint.exists(new_collection.id):
            logger.info(
                f"Collection '{new_collection.id}' already exists on the STAC API. "
                "Merging extents and updating."
            )
            existing = self._collections_endpoint.get(new_collection.id)
            _merge_extents(new_collection, existing)
            self._collections_endpoint.update(new_collection)
        else:
            logger.info(f"Collection '{new_collection.id}' does not exist on the STAC API. Creating.")
            new_collection.validate()
            self._collections_endpoint.create(new_collection)

        return new_collection

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

        # Ensure all hrefs in the item are uri's
        for asset in item.assets.values():
            asset.href = UPath(asset.href).as_uri()

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")


def _merge_extents(target: Collection, source: Collection) -> None:
    """Expand *target*'s spatial and temporal extents to also cover *source*'s extents.

    Only *target* is modified in place.  *source* is not changed.

    For temporal extents, ``None`` represents an open/unbounded boundary:
    - A ``None`` start means "no lower bound (earliest possible time)".
    - A ``None`` end means "no upper bound (ongoing)".
    When merging, if either collection has an unbounded boundary the merged extent also
    has an unbounded boundary on that side.

    :param target: The collection whose extents will be expanded.
    :param source: The collection whose extents are used as additional coverage.
    """
    import datetime as _dt

    # --- Spatial extent ---
    target_bbox = list(target.extent.spatial.bboxes[0])  # [west, south, east, north]
    source_bbox = list(source.extent.spatial.bboxes[0])
    merged_bbox = [
        min(target_bbox[0], source_bbox[0]),  # west  – smaller is further west
        min(target_bbox[1], source_bbox[1]),  # south – smaller is further south
        max(target_bbox[2], source_bbox[2]),  # east  – larger is further east
        max(target_bbox[3], source_bbox[3]),  # north – larger is further north
    ]
    target.extent.spatial = SpatialExtent([merged_bbox])

    # --- Temporal extent ---
    target_interval = target.extent.temporal.intervals[0]
    source_interval = source.extent.temporal.intervals[0]

    def _to_utc(value):
        """Make a datetime timezone-aware (UTC) so it can be compared with other datetimes."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=_dt.timezone.utc)
        return value

    t_start = _to_utc(target_interval[0])
    t_end = _to_utc(target_interval[1])
    s_start = _to_utc(source_interval[0])
    s_end = _to_utc(source_interval[1])

    # None means unbounded: if either side is None (unbounded), the merged result is also None.
    merged_start = None if (t_start is None or s_start is None) else min(t_start, s_start)
    merged_end = None if (t_end is None or s_end is None) else max(t_end, s_end)
    target.extent.temporal = TemporalExtent([[merged_start, merged_end]])
