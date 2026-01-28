import logging
from pathlib import Path
from typing import Optional, List
import json
import requests

from pystac import Item, Collection
from pystac_client import Client

from stacbuilder.collector import IDataCollector
logger = logging.getLogger(__name__)

class STACItemCollector(IDataCollector):
    """Collects files that match a glob, in a directory.

    Note that the values None and [] have a different meaning for self.input_files:
    See :meth: FileCollector.has_collected
    """

    def __init__(self, collection_url:str,max_files: int = -1) -> None:
        """Initialize the FileCollector with an input directory, glob pattern, and maximum number of files.

        :param input_dir: The directory to search for files.
        :param glob: The glob pattern to match files. The default is "*", which matches all files.
        :param max_files: The maximum number of files to collect. Default is -1, which means no limit.
        """
        self.collection_url: str = collection_url

        self.max_files: int = max_files if max_files is not None else -1
        self._items: Optional[List[Item]] = None


    def collect(self):
        """Collect files matching the glob pattern in the input directory. Once collected, the files can be accessed via the `input_files` property."""
        logger.info(f"START collecting files in '{self.collection_url}''")

        stac_collection = Collection.from_file(self.collection_url)
        cache_path = Path(f"/tmp/stac_items_{stac_collection.id}.json")

        if cache_path.exists():
            with open(cache_path, "r") as f:
                items_dicts = json.load(f)
                data_items = [Item.from_dict(d) for d in items_dicts[:self.max_files]]
        else:


            #stac_client = Client.open(stac_collection.get_root().get_self_href())

            # Search the collection with a limit on the number of items
            #put in default datetime filter: remove invalid items without time
            #search = stac_client.search(collections = stac_collection.id, datetime="1950-01-02T00:00:00.000Z/..", limit=self.max_files)

            items_link = self.collection_url + "/items?httpAccept=application/geo%2Bjson;profile=https://stacspec.org"
            item_json = requests.get(items_link).json()

            def parse_item(item_dict):
                try:
                    return Item.from_dict(item_dict)
                except Exception as e:
                    logger.warning(f"Failed to parse item: {e} - {item_dict}")
                    return None

            items = [ parse_item(i) for i in item_json["features"]]
            items = [i for i in items if i is not None]
            # Retrieve the items
            #items: list[Item] = list(search.items())

            # some (bad) collections have items without actual data
            data_items: list[Item] = [p for p in items for (k, a) in p.assets.items() if
                                      "data" in a.roles and "application/x-netcdf" == a.media_type]
            data_items = [i for i in data_items if i.collection_id == stac_collection.id]

            with open(cache_path, "w") as f:
                json.dump([item.to_dict() for item in data_items], f, indent=2)



        self._items = data_items
        logger.info(f"DONE collecting files. Found {len(self._items)} files.")

    def has_collected(self) -> bool:
        """Whether or not the data has been collected.

        Note that the values None and [] have a different meaning here:
        Namely:
            `self.input_files == []`
            means no files were found during collection,
        but in contrast:
            `self.input_files is None`
            means the collection was never run in the first place.
        """
        return self._items is not None

    def reset(self):
        """Reset the collector: clear the collected files."""
        self._items = None

    @property
    def input_items(self) -> List[Item]:
        """Get the collected input files."""
        return self._items or []