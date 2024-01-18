"""
This is a module, for a temporary fix and it will be removed when the proper fix has been deployed.

This utility module helps us to convert a timezone format that, at present,
the openeo-geopyspark-driver doesn't support.
This root cause is that the method datetime.fromisoformat in Python *version 3.8*
does not yet support this ISO format.
https://docs.python.org/3/library/datetime.html#datetime.datetime.fromisoformat
However higher versions of Python fix that issue, but we don't want to force
all users of openeo-geopyspark-driver to migrate to Python >3.8 just for this one issue.


See GitHub issue:
https://github.com/Open-EO/openeo-geopyspark-driver/issues/568

And PR to fix this:
https://github.com/Open-EO/openeo-geopyspark-driver/pull/615
"""

import copy
import datetime as dt
import json
import logging

from pathlib import Path
from typing import Any, Dict, Iterable


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
