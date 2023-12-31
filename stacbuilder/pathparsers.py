# TODO: rename and restructure: this module contains mostly InputPathParsers.
#   It was named core because a lot of other modules will import from here
#   and we need to prevent circular imports; but that doesn't say what it
#   really does anymore.

import abc
import calendar
import datetime as dt
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


from stacbuilder.config import InputPathParserConfig


class UnknownInputPathParserClass(Exception):
    def __init__(self, classname: str, *args: object) -> None:
        message = f"There is no implementing class for this class name: {classname}"
        super().__init__(message, *args)


class InputPathParserFactory:

    _implementations = {}

    @classmethod
    def register(cls, parser_class: type):
        name = parser_class.__name__
        cls._implementations[name] = parser_class

    @classmethod
    @property
    def implementation_names(cls):
        return sorted(cls._implementations.keys())

    @classmethod
    def from_config(cls, config: InputPathParserConfig):
        if config.classname not in cls._implementations:
            raise UnknownInputPathParserClass(config.classname)

        params = config.parameters or {}
        return cls._implementations[config.classname](**(params))


class InputPathParser(abc.ABC):
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        InputPathParserFactory.register(cls)

    @abc.abstractmethod
    def parse(self, input_file: Path) -> Dict[str, Any]:
        return {}


class NoopInputPathParser(InputPathParser):
    def parse(self, input_file: Path) -> Dict[str, Any]:
        return {}


TypeConverter = Callable[[str], Any]
TypeConverterMapping = Dict[str, TypeConverter]


class RegexInputPathParser(InputPathParser):
    def __init__(
        self,
        regex_pattern: Union[str, re.Pattern],
        type_converters: Optional[TypeConverterMapping] = None,
        fixed_values: Optional[Dict[str, Any]] = None,
    ):
        if isinstance(regex_pattern, re.Pattern):
            self._regex = regex_pattern
        else:
            self._regex = re.compile(regex_pattern)

        self._type_converters = type_converters or {}
        self._fixed_values = fixed_values or {}

        self._data = None
        self._path = None

    def parse(self, input_file: Union[Path, str]) -> Dict[str, Any]:
        data = {}
        self._path = str(input_file)

        match = self._regex.search(self._path)
        if match:
            data = match.groupdict()

        for key, value in data.items():
            if key in self._type_converters:
                func = self._type_converters[key]
                data[key] = func(value)

        for key, value in self._fixed_values.items():
            data[key] = value

        self._data = data
        self._post_process_data()

        import pprint

        print(f"{input_file=}")
        pprint.pprint(self._data)
        print()

        return self._data

    def _post_process_data(self):
        pass

    @property
    def data(self):
        return self._data

    @property
    def regex(self):
        return self._regex

    @property
    def type_converters(self):
        return self._type_converters


class LandsatNDWIInputPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        super().__init__(type_converters=type_converters, *args, **kwargs)

    def _post_process_data(self):
        start_dt = self._get_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._get_end_datetime()

    def _get_start_datetime(self):
        return dt.datetime(self._data["year"], 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self):
        start_dt = self._get_start_datetime()
        year = start_dt.year
        # month = start_dt.month
        # end_month = calendar.monthrange(year, month)[1]
        return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)


class PeopleEAIncaCFactorInputPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        regex_pattern = ".*/PEOPLE_INCA_c-factor_(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2}).*\\.tif$"
        fixed_values = {"band": "cfactor"}
        super().__init__(
            regex_pattern=regex_pattern, type_converters=type_converters, fixed_values=fixed_values, *args, **kwargs
        )

    def _post_process_data(self):
        start_dt = self._get_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._get_end_datetime()

    def _get_start_datetime(self):
        year = self._data.get("year")
        month = self._data.get("month")
        day = self._data.get("day")
        print(f"DEBUG: {year=}, {month=}, {day=}, {self._data=}, {self._path=}")

        if not (year and month and day):
            print(
                "WARNING: Could not find all date fields: "
                + f"{year=}, {month=}, {day=}, {self._data=},\n{self._path=}\n{self._regex.pattern=}"
            )
            return None

        return dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self):
        start_dt = self._get_start_datetime()
        if not start_dt:
            print(
                "WARNING: Could not determine start_datetime: " + f"{self._data=}, {self._path=}, {self._regex.pattern}"
            )
            return None

        year = start_dt.year
        return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)


class ERA5LandInputPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        super().__init__(type_converters=type_converters, *args, **kwargs)

    def _post_process_data(self):
        start_dt = self._get_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._get_end_datetime()

    def _get_start_datetime(self):
        return dt.datetime(self._data["year"], self._data["month"], self._data["day"], 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self):
        start_dt = self._get_start_datetime()
        year = start_dt.year
        month = start_dt.month
        end_month = calendar.monthrange(year, month)[1]
        return dt.datetime(year, month, end_month, 23, 59, 59, tzinfo=dt.timezone.utc)


class ANINPathParser(InputPathParser):
    def parse(self, input_file: Path) -> Dict[str, Any]:

        #
        # Example:
        #
        # filename:  reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800101.tif
        # root: reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800101
        # item_id is same as root
        # start_date = 1980-01-01
        # start_datetime = 1980-01-01T00:00:00Z
        # end_datetime = last second of the end of the month

        input_file = Path(input_file)
        root = input_file.stem
        file_parts = root.split("_")
        band = "_".join(file_parts[1:4])

        date_string = file_parts[-1]
        year = int(date_string[0:4])
        month = int(date_string[4:6])
        day = int(date_string[6:8])
        start_datetime = dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.timezone.utc)
        end_month = calendar.monthrange(year, month)[1]
        end_datetime = dt.datetime(year, month, end_month, 23, 59, 59, tzinfo=dt.timezone.utc)

        info = {}
        info["item_id"] = root
        info["datetime"] = start_datetime
        info["start_datetime"] = start_datetime
        info["end_datetime"] = end_datetime
        info["band"] = band
        info["item_type"] = band

        return info
