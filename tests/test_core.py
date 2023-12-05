import datetime as dt


import pytest


from stacbuilder.core import InputPathParserFactory, RegexInputPathParser
from stacbuilder.core import UnknownInputPathParserClass
from stacbuilder.config import InputPathParserConfig


def test_factory():
    expected_names = sorted(
        ["NoopInputPathParser", "RegexInputPathParser", "ANINRegexInputPathParser", "ANINPathParser"]
    )
    assert InputPathParserFactory.implementation_names == expected_names


def test_regexinputpathparser():
    path = "/data/foo/bar/tile9876_band-name1-567XyZ_2034-12-31T02:03:59Z.tif"
    regex = r".*/tile(?P<tile>\d+)_(?P<band>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}Z)\.tif$"
    parser = RegexInputPathParser(
        regex_pattern=regex,
    )

    data = parser.parse(path)

    assert "tile" in data
    assert "band" in data
    assert "datetime" in data
    assert data["tile"] == "9876"
    assert data["band"] == "band-name1-567XyZ"
    assert data["datetime"] == "2034-12-31T02:03:59Z"


def test_regexinputpathparser_converts_types_correctly():
    path = "/data/foo/bar/tile9876_band-name1-567XyZ_2034-12-31T02:03:59Z.tif"
    regex = r".*/tile(?P<tile>\d+)_(?P<band>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}Z)\.tif$"
    converters = {"tile": int, "datetime": dt.datetime.fromisoformat}
    parser = RegexInputPathParser(regex_pattern=regex, type_converters=converters)

    data = parser.parse(path)

    assert "tile" in data
    assert "band" in data
    assert "datetime" in data
    assert data["tile"] == 9876
    assert data["band"] == "band-name1-567XyZ"
    assert data["datetime"] == dt.datetime(2034, 12, 31, 2, 3, 59, tzinfo=dt.UTC)


def test_parser_factory_raises_exc_when_class_not_in_register():
    config = InputPathParserConfig(
        classname="ClassDoesNotExist",
    )
    with pytest.raises(UnknownInputPathParserClass):
        InputPathParserFactory.from_config(config)


def test_construct_regexinputpathparser_from_config():
    pattern = (
        r".*/tile(?P<tile>\d+)_(?P<band>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}Z)\.tif$"
    )
    config = InputPathParserConfig(
        classname="RegexInputPathParser",
        parameters={
            "regex_pattern": pattern,
        },
    )

    parser = InputPathParserFactory.from_config(config)
    assert isinstance(parser, RegexInputPathParser)
    assert parser.regex.pattern == pattern


def test_construct_regexinputpathparser_from_json():
    data = {
        "classname": "ANINRegexInputPathParser",
        "parameters": {
            "regex_pattern": ".*/reanalysis-era5-land-monthly-means_(?P<band>[a-zA-Z0-9\\_]+_monthly)_(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2})\\.tif$",
        },
    }
    config = InputPathParserConfig(**data)
    parser = InputPathParserFactory.from_config(config)
    assert isinstance(parser, RegexInputPathParser)
    assert (
        parser.regex.pattern
        == ".*/reanalysis-era5-land-monthly-means_(?P<band>[a-zA-Z0-9\\_]+_monthly)_(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2})\\.tif$"
    )
