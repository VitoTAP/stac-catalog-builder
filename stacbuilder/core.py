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

    # @classmethod
    # def create_parser(cls, name: str, params: Optional[Dict[str, Any]] = None):
    #     params = params or {}
    #     return cls._implementations[name](**params)

    #     # if name in cls._implementations:
    #     #     return cls._implementations[name](**params)
    #     # else:
    #     #     return NoopInputPathParser()

    @classmethod
    def from_config(cls, config: InputPathParserConfig):

        if config.classname not in cls._implementations:
            raise UnknownInputPathParserClass(config.classname)

        params = config.parameters or {}
        return cls._implementations[config.classname](**(params))

        # if config.classname in cls._implementations:
        #     params = config.parameters
        #     return cls._implementations[config.classname](**(params))
        # else:
        #     return NoopInputPathParser()


# InputPathParser: Callable[[str], Dict[str, Any]]

# # TODO: simplify, maybe with a Protocol instead of an abstract base class
# class InputPathParser(abc):

#     def __init__(self):
#         self._input_file: Path = None
#         self._extracted_info: Dict[str, Any] = {}

#     def parse(self, input_file: Path) -> Dict[str, Any]:
#         self._input_file = input_file
#         self._extracted_info = {}
#         return self._extracted_info

#     @abc.abstractmethod
#     def _parse(self):
#         pass

#     @property
#     def input_file(self) -> Path:
#         return self._input_file

#     @property
#     def info(self) -> Dict[str, Any]:
#         return self._extracted_info


# TODO: simplify, maybe with a Protocol instead of an abstract base class
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
        fields: List[str],
        type_converters: Optional[TypeConverterMapping] = None,
    ):
        self._fields = fields
        if isinstance(regex_pattern, re.Pattern):
            self._regex = regex_pattern
        else:
            self._regex = re.compile(regex_pattern)

        self._type_converters = type_converters or {}

    def parse(self, input_file: Union[Path, str]) -> Dict[str, Any]:
        data = {}
        input_file_str = str(input_file)

        match = self._regex.search(input_file_str)
        if match:
            data = match.groupdict()

        for key, value in data.items():
            if key in self._type_converters:
                func = self._type_converters[key]
                data[key] = func(value)

        return data

    @property
    def fields(self):
        return self._fields

    @property
    def regex(self):
        return self._regex

    @property
    def type_converters(self):
        return self._type_converters


class ANINPathParser(InputPathParser):
    def parse(self, input_file: Path) -> Dict[str, Any]:

        # Example:

        # filename:  reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800101.tif
        # root: reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800101
        # item_id is same as root
        # start_date = 1980-01-01
        # start_datetime = 1980-01-01T00:00:00Z
        # end_datetime = last second of the end of the month

        input_file = Path(input_file)
        root = input_file.stem
        file_parts = root.split("_")
        band = file_parts[1]

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

        return info
