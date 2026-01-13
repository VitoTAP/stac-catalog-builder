"""Classes that parse input paths and extract metadata from the file name or file path.

Normally these are paths to GeoTIFF files, but this could include other file formats in the future.
"""

import abc
import calendar
import datetime as dt
import re
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from loguru import logger

from stacbuilder.config import InputPathParserConfig


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

    @property
    def implementation_names(self):
        """Get the class names of all known implementations."""
        return sorted(self._implementations.keys())

    @classmethod
    def from_config(cls, config: InputPathParserConfig):
        """Create an InputPathParser from a InputPathParserConfig.
        Returns None if no input_path_parser is configured."""
        if not config:
            return None

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


# Type alias for the callables we need, what signature the function/method must have.
TypeConverter = Callable[[str], Any]

# Type alias for the converting functions.
# These convert strings extracted from the path into a more useful time.
# For example a year, month or day can be converted to an integer, or an entire
# date could be converted to datetime.
TypeConverterMapping = Dict[str, TypeConverter]


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
        if not self._regex_has_named_groups():
            logger.warning(
                f"The regex pattern does not have named groups: {self._regex.pattern}. Path parsing will be skipped and only fixed values will be used."
            )

        self._type_converters = type_converters or {}
        self._fixed_values = fixed_values or {}

    def parse(self, input_file: Union[Path, str]) -> Dict[str, Any]:
        data = {}
        if isinstance(input_file, str):
            input_file = Path(input_file)
        path = input_file.as_posix()

        if self._regex_has_named_groups():
            match = self._regex.search(path)
            if match:
                data = match.groupdict()
            else:
                logger.warning(
                    f"No data could be extracted from this path: {path}, " + f"regex pattern={self._regex.pattern}"
                )

        for key, value in self._fixed_values.items():
            if isinstance(value, str) and "{" in value and "}" in value:
                try:
                    value = value.format(**data)
                except KeyError as e:
                    logger.warning(f"Could not format fixed value '{value}' with data keys: {e}")
            data[key] = value

        for key, value in data.items():
            if key in self._type_converters:
                func = self._type_converters[key]
                data[key] = func(value)

        data = self._post_process_data(data, path)

        return data

    def _post_process_data(self, data: Dict[str, Any], path: str) -> Dict[str, Any]:
        """Optionally do any desired processing on the extracted data."""
        return data

    @property
    def regex(self) -> re.Pattern:
        """Get the regular expression used to parse the path."""
        return self._regex

    @property
    def type_converters(self) -> TypeConverterMapping:
        """Return a"""
        return self._type_converters

    def _regex_has_named_groups(self):
        if not self._regex:
            raise ValueError("The regex pattern is not set.")
        return re.search(r"\(\?P<", self._regex.pattern) is not None


class Period(Enum):
    """Enum for the possible periods that can be used in the DefaultInputPathParser."""

    YEARLY = "yearly"
    MONTHLY = "monthly"
    DAILY = "daily"
    SECONDLY = "secondly"


class DefaultInputPathParser(RegexInputPathParser):
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
        period = period.lower() or "daily"
        match period:
            case "yearly":
                self._period = Period.YEARLY
            case "monthly":
                self._period = Period.MONTHLY
            case "daily":
                self._period = Period.DAILY
            case "secondly":
                self._period = Period.SECONDLY
            case _:
                raise ValueError(f"Period argument {period} must be one of {[p.value for p in Period]}")

    @property
    def period(self) -> Period:
        return self._period

    def _fill_missing_data(self, data: Dict[str, Any]) -> None:
        if self.period is Period.YEARLY and "month" not in data:
            data["month"] = 1
        if (self.period is Period.YEARLY or self.period is Period.MONTHLY) and "day" not in data:
            data["day"] = 1

    def _post_process_data(self, data: Dict[str, Any], path: str) -> Dict[str, Any]:
        self._fill_missing_data(data)
        start_dt = self._get_start_datetime(data)
        data["datetime"] = start_dt
        data["start_datetime"] = start_dt
        data["end_datetime"] = self._get_end_datetime(data)
        return data

    def _get_start_datetime(self, data: Dict[str, Any]):
        return dt.datetime(
            year=data["year"],
            month=data["month"],
            day=data["day"],
            hour=int(data.get("hour", 0)),
            minute=int(data.get("minute", 0)),
            second=int(data.get("second", 0)),
            tzinfo=dt.timezone.utc,
        )

    def _get_end_datetime(self, data: Dict[str, Any]):
        start_dt = self._get_start_datetime(data)
        year = start_dt.year

        match self.period:
            case Period.YEARLY:
                return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)
            case Period.MONTHLY:
                month = start_dt.month
                end_month = calendar.monthrange(year, month)[1]
                return dt.datetime(year, month, end_month, 23, 59, 59, tzinfo=dt.timezone.utc)
            case Period.SECONDLY:
                return start_dt + dt.timedelta(seconds=1)
            case _:
                return dt.datetime(year, start_dt.month, start_dt.day, 23, 59, 59, tzinfo=dt.timezone.utc)


class LandsatNDWIInputPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        super().__init__(type_converters=type_converters, *args, **kwargs)

    def _post_process_data(self, data: Dict[str, Any], path: str) -> Dict[str, Any]:
        start_dt = self._get_start_datetime(data)
        data["datetime"] = start_dt
        data["start_datetime"] = start_dt
        data["end_datetime"] = self._get_end_datetime(data)
        return data

    def _get_start_datetime(self, data: Dict[str, Any]):
        return dt.datetime(data["year"], 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self, data: Dict[str, Any]):
        start_dt = self._get_start_datetime(data)
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

    def _post_process_data(self, data: Dict[str, Any], path: str) -> Dict[str, Any]:
        start_dt = self._derive_start_datetime(data, path)
        data["datetime"] = start_dt
        data["start_datetime"] = start_dt
        data["end_datetime"] = self._derive_end_datetime(data, path)
        return data

    def _derive_start_datetime(self, data: Dict[str, Any], path: str):
        """Derive the start datetime from other properties that were extracted."""
        year = data.get("year")
        month = data.get("month")
        day = data.get("day")

        if not (year and month and day):
            logger.warning(
                f"Could not find all date fields: {year=}, {month=}, {day=}, {data=}, {path=}, {self._regex.pattern=}"
            )
            return None

        return dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _derive_end_datetime(self, data: Dict[str, Any], path: str):
        """Derive the end datetime from other properties that were extracted."""
        start_dt = self._derive_start_datetime(data, path)
        if not start_dt:
            logger.warning(f"Could not determine start_datetime: {data=}, {path=}, {self._regex.pattern}")
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

    def _post_process_data(self, data: Dict[str, Any], path: str) -> Dict[str, Any]:
        start_dt = self._get_start_datetime(data)
        data["datetime"] = start_dt
        data["start_datetime"] = start_dt
        data["end_datetime"] = self._get_end_datetime(data)
        return data

    def _get_start_datetime(self, data: Dict[str, Any]):
        return dt.datetime(data["year"], data["month"], data["day"], 0, 0, 0, tzinfo=dt.timezone.utc)

    def _get_end_datetime(self, data: Dict[str, Any]):
        start_dt = self._get_start_datetime(data)
        year = start_dt.year
        month = start_dt.month
        end_month = calendar.monthrange(year, month)[1]
        return dt.datetime(year, month, end_month, 23, 59, 59, tzinfo=dt.timezone.utc)
