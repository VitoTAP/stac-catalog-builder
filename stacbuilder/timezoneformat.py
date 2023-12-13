import copy
import datetime as dt
import json
import logging
import shutil

from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable


from openeo.util import rfc3339


_logger = logging.getLogger(__name__)


class TimezoneFormatConverter:
    def convert_collection(self, in_path: Path, out_path: Path) -> None:
        with open(in_path, "r") as f_in:
            data = json.load(f_in)

        data = self._convert_collection_dict(data)

        with open(out_path, "w") as f_out:
            json.dump(data, f_out, indent=2)

    def convert_item(self, in_path: Path, out_path: Path) -> None:
        _logger.debug(f"Converting STAC item from {in_path} to {out_path} ...")
        with open(in_path, "r") as f_in:
            data = json.load(f_in)

        data = self._convert_item_dict(data)

        with open(out_path, "w") as f_out:
            json.dump(data, f_out, indent=2)
        _logger.debug(f"DONE: converted STAC item from {in_path} to {out_path}")

    def _convert_collection_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        converted = copy.deepcopy(data)
        temporal_extent = converted["extent"]["temporal"]["interval"]
        converted["extent"]["temporal"]["interval"] = self._convert_value(temporal_extent)
        return converted

    def _convert_item_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        converted = copy.deepcopy(data)

        props_to_convert = ["datetime", "start_datetime", "end_datetime"]
        props = converted.get("properties")
        if props:
            for prop_name in props_to_convert:
                val = props.get(prop_name)
                if val is not None:
                    converted["properties"][prop_name] = self._convert_value(val)

        return converted

    def _convert_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._convert_datetime(value)
        elif isinstance(value, list):
            return [self._convert_value(x) for x in value]
        else:
            return value

    def _convert_datetime(self, dt_string: str) -> str:
        """Convert UTC datetime strings that encode UTC timezone with "Z"."""
        try:
            the_datetime = rfc3339.parse_datetime(dt_string)
        except ValueError:
            return dt_string
        else:
            return self.datetime_to_str_no_z(the_datetime)

    def datetime_to_str_no_z(self, timestamp: dt.datetime, timespec: str = "auto") -> str:
        """Converts a :class:`datetime.datetime` instance to an ISO8601 string in the
        `RFC 3339, section 5.6
        <https://datatracker.ietf.org/doc/html/rfc3339#section-5.6>`__ format required by
        the :stac-spec:`STAC Spec <master/item-spec/common-metadata.md#date-and-time>`.

        Args:
            dt : The datetime to convert.
            timespec: An optional argument that specifies the number of additional
                terms of the time to include. Valid options are 'auto', 'hours',
                'minutes', 'seconds', 'milliseconds' and 'microseconds'. The default value
                is 'auto'.

        Returns:
            str: The ISO8601 (RFC 3339) formatted string representing the datetime.
        """
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)

        timestamp_str = timestamp.isoformat(timespec=timespec)
        if timestamp_str.endswith("Z"):
            timestamp_str = "{}+00:00".format(timestamp_str[:-1])

        return timestamp_str

    # def OLD_post_process_catalog(
    #     self,
    #     in_coll_path: Path,
    #     item_glob: str,
    #     converted_dir: Optional[Path] = None,
    # ) -> None:
    #     """Run post-processing steps on the STAC catalog.

    #     This is a temporary fix until we can make openeo-geopyspark-driver's
    #     load_stac accept the UTC timestamps ending in "Z".
    #     """
    #     if not in_coll_path.exists():
    #         raise FileNotFoundError(
    #             'Input collection file for argument "in_coll_path" does not exist: '
    #             + f"{in_coll_path=}"
    #         )
    #     if not in_coll_path.is_file():
    #         raise Exception(
    #             'Input collection file for argument "in_coll_path" must be a file, '
    #             + f"not a directory. {in_coll_path=}"
    #         )

    #     in_place = not converted_dir
    #     converted_out_dir: Path = converted_dir or in_coll_path.parent
    #     if in_place:
    #         collection_converted_path = in_coll_path
    #     else:
    #         collection_converted_path = converted_out_dir / in_coll_path.name
    #         if converted_out_dir.exists():
    #             shutil.rmtree(converted_out_dir)
    #         converted_out_dir.mkdir(parents=True)

    #     self.convert_collection(in_coll_path, collection_converted_path)

    #     self._process_item_files(in_coll_path.parent, converted_out_dir, item_glob)

    def process_catalog(
        self,
        in_coll_path: Path,
        in_item_paths: Iterable[Path],
        output_dir: Path,
    ) -> None:
        """Run post-processing steps on the STAC catalog.

        This is a temporary fix until we can make openeo-geopyspark-driver's
        load_stac accept the UTC timestamps ending in "Z".
        """
        in_coll_path = Path(in_coll_path)

        if not in_coll_path.exists():
            raise FileNotFoundError('Argument "in_coll_path": input collection does not exist: ' + f"{in_coll_path=}")
        if not in_coll_path.is_file():
            raise Exception(
                'Argument "in_coll_path": input collection must be a file, ' + f"not a directory. {in_coll_path=}"
            )

        # In this case, creating the output directory should be done by the
        # calling code because there might be other processing as well, and
        # otherwise a mess of duplicate code.
        if not output_dir.exists():
            raise FileNotFoundError('Argument "output_dir": output directory does not exist: ' + f"{in_coll_path=}")
        if not output_dir.is_dir():
            raise FileNotFoundError('Argument "output_dir": output directory must be a directory:' + f"{in_coll_path=}")

        bad_items = [ip for ip in in_item_paths if not Path(ip).exists()]
        if bad_items:
            raise Exception("Following STAC item paths don't exist: " + f"{bad_items}")

        print("=== item_files_in: ===")
        for f in in_item_paths:
            print(f)
        print("\n")

        out_collection_path = output_dir / in_coll_path.name
        self.convert_collection(in_coll_path, out_collection_path)

        num_files = len(in_item_paths)
        for i, item_path in enumerate(in_item_paths):
            rel_path = item_path.relative_to(in_coll_path.parent)
            out_path = output_dir / rel_path
            print(f"PROGRESS: converting STAC item {i+1} of {num_files}:\n{item_path}\nto:\n{out_path}")
            self.convert_item(item_path, out_path)

    # def _process_item_files(self, collection_dir: Path, converted_dir: Path, glob_pattern: str) -> None:
    #     """Convert each STAC item file found in the subfolders per year"""

    #     in_place = collection_dir.samefile(converted_dir)
    #     item_files_in = list(collection_dir.glob(glob_pattern))

    #     item_folders_in: List[Path] = list(set(i.parent for i in item_files_in))
    #     item_folders_out = [
    #         converted_dir / i.relative_to(collection_dir) for i in item_folders_in
    #     ]
    #     print("=== item_files_in: ===")
    #     for f in item_files_in:
    #         print(f)
    #     print("\n")

    #     if not in_place:
    #         for folder in item_folders_out:
    #             folder.mkdir(parents=True)

    #     num_files = len(item_files_in)
    #     for i, item_path in enumerate(item_files_in):
    #         rel_path = item_path.relative_to(collection_dir)
    #         out_path = converted_dir / rel_path

    #         print(
    #             f"PROGRESS: converting STAC item {i+1} of {num_files}:\n{item_path}\nto: {out_path=}"
    #         )
    #         print(f"{item_path=}")
    #         print(f"{rel_path=}")
    #         print(f"{out_path=}")
    #         self.convert_item(item_path, out_path)
