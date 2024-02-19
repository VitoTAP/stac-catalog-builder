"""Classes that parse input paths and extract metadata from the file name or file path.

Normally these are paths to GeoTIFF files, but this could include other file formats in the future.
"""

import abc
import calendar
import datetime as dt
import logging
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union


from stacbuilder.config import InputPathParserConfig


logger = logging.getLogger(__name__)


class UnknownInputPathParserClass(Exception):
    def __init__(self, classname: str, *args: object) -> None:
        message = f"There is no implementing class for this class name: {classname}"
        super().__init__(message, *args)


class InputPathParserFactory:
    """Constructs an InputPathParser that matches class name.

    In the CollectionConfig we can configure which InputPathParser to use for
    that particular dataset. This factory makes it possible to find and
    instantiate that class.

    Optionally you can also configure some parameters to pass to the constructor,
    but this is a but cumbersome to configure. In general it is easier and cleaner
    to create a subclass of InputPathParserFactory (or one of its general subclasses)
    where you set all the parameters hard coded for that specific STAC collection.

    All subclasses of InputPathParserFactory automatically register themselves
    in the InputPathParserFactory, via the __init_subclass__ in their abstract
    base class, InputPathParser.
    """

    _implementations = {}

    @classmethod
    def register(cls, parser_class: type):
        """Register a new subclass as a InputPathParser, so the factory knows
        all the classes it can instantiate."""
        name = parser_class.__name__
        cls._implementations[name] = parser_class

    @classmethod
    @property
    def implementation_names(cls):
        """Get the class names of all known implementations."""
        return sorted(cls._implementations.keys())

    @classmethod
    def from_config(cls, config: InputPathParserConfig):
        if config.classname not in cls._implementations:
            raise UnknownInputPathParserClass(config.classname)

        params = config.parameters or {}
        return cls._implementations[config.classname](**(params))


class InputPathParser(abc.ABC):
    """Abstract Base Class for all input path parsers."""

    @classmethod
    def __init_subclass__(cls) -> None:
        """This method is called whenever the containing class is subclassed.

        All subclasses of InputPathParserFactory automatically register themselves
        in the InputPathParserFactory, via the __init_subclass__ in their abstract
        base class, InputPathParser.
        """
        super().__init_subclass__()
        InputPathParserFactory.register(cls)

    @abc.abstractmethod
    def parse(self, input_file: Path) -> Dict[str, Any]:
        """Parse the path to an input file to extract metadata from the file path."""
        return {}


class NoopInputPathParser(InputPathParser):
    """A dummy InputPathParser that never extracts anything from the path.

    Not every dataset will need path parsing and this class makes it easier
    deal with that. Otherwise you need to check for `None` everywhere.
    """

    def parse(self, input_file: Path) -> Dict[str, Any]:
        return {}


TypeConverter = Callable[[str], Any]
"""Type alias for the callables we need, what signature the function/method must have."""

TypeConverterMapping = Dict[str, TypeConverter]
"""Type alias for the converting functions.

These convert strings extracted from the path into a more useful time.
For example a year, month or day can be converted to an integer, or an entire
date could be converted to datetime.
"""


class RegexInputPathParser(InputPathParser):
    """Path parser that uses regular expressions to extract properties from a path."""

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
        else:
            logger.warning(
                f"No data could be extracted from this path: {self._path}, " + f"regex pattern={self._regex.pattern}"
            )

        for key, value in data.items():
            if key in self._type_converters:
                func = self._type_converters[key]
                data[key] = func(value)

        for key, value in self._fixed_values.items():
            data[key] = value

        self._data = data
        self._post_process_data()

        return self._data

    def _post_process_data(self):
        """Optionally do any desired processing on the extracted data."""
        pass

    @property
    def data(self) -> Dict[str, Any]:
        """Get the data extracted from the path."""
        return self._data

    @property
    def regex(self) -> re.Pattern:
        """Get the regular expression used to parse the path."""
        return self._regex

    @property
    def type_converters(self) -> TypeConverterMapping:
        """Return a"""
        return self._type_converters


class GeneralInputPathParser(RegexInputPathParser):
    """Path parser that uses regular expressions to extract properties from a path.
    This input parser overrides the datetime, start_datetime and end_datetime properties.
    """

    def __init__(self, period: Optional[str] = None, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        self._set_period(period)
        super().__init__(type_converters=type_converters, *args, **kwargs)

    def _set_period(self, period: Optional[str] = None):
        period = period or "Daily"

        possible_periods = ["Yearly", "Monthly", "Daily"]
        if period not in possible_periods:
            raise ValueError(f"Period must be one of {possible_periods}")
        self._period = period

    @property
    def period(self):
        return self._period

    def _fill_missing_data(self):
        if self._period == "Yearly" and "month" not in self._data:
            self._data["month"] = 1
        if (self.period == "Yearly" or self.period == "Monthly") and "day" not in self._data:
            self._data["day"] = 1

    def _post_process_data(self):
        self._fill_missing_data()
        start_dt = self._get_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._get_end_datetime()

    def _get_start_datetime(self):
        return dt.datetime(self._data["year"], self._data["month"], self._data["day"], 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self):
        start_dt = self._get_start_datetime()
        year = start_dt.year

        if self._period == "Yearly":
            return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)
        elif self._period == "Monthly":
            month = start_dt.month
            end_month = calendar.monthrange(year, month)[1]
            return dt.datetime(year, month, end_month, 23, 59, 59, tzinfo=dt.timezone.utc)
        else:
            return dt.datetime(year, start_dt.month, start_dt.day, 23, 59, 59, tzinfo=dt.timezone.utc)


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
        return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)


class PeopleEAIncaCFactorInputPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        regex_pattern = ".*/PEOPLE_INCA_c-factor_(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2}).*\\.tif$"
        fixed_values = {"asset_type": "cfactor"}
        super().__init__(
            regex_pattern=regex_pattern, type_converters=type_converters, fixed_values=fixed_values, *args, **kwargs
        )

    def _post_process_data(self):
        start_dt = self._derive_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._derive_end_datetime()

    def _derive_start_datetime(self):
        """Derive the start datetime from other properties that were extracted."""
        year = self._data.get("year")
        month = self._data.get("month")
        day = self._data.get("day")

        if not (year and month and day):
            print(
                "WARNING: Could not find all date fields: "
                + f"{year=}, {month=}, {day=}, {self._data=},\n{self._path=}\n{self._regex.pattern=}"
            )
            return None

        return dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _derive_end_datetime(self):
        """Derive the end datetime from other properties that were extracted."""
        start_dt = self._derive_start_datetime()
        if not start_dt:
            print(
                "WARNING: Could not determine start_datetime: " + f"{self._data=}, {self._path=}, {self._regex.pattern}"
            )
            return None

        year = start_dt.year
        return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)


class ERA5LandInputPathParser(RegexInputPathParser):
    """Path parser for 2 datasets:
    reanalysis-era5-land_southafrica and
    ERA5-Land-monthly-averaged-data-v2

    See also:
        configs-datasets/ANIN/reanalysis-era5-land_southafrica
        configs-datasets/ANIN/reanalysis-era5-land_southafrica
    """

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
