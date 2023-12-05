import datetime as dt
from typing import List

import pytest


from stacbuilder.core import (
    InputPathParserFactory,
    RegexInputPathParser,
    ERA5LandInputPathParser,
    UnknownInputPathParserClass,
)
from stacbuilder.config import InputPathParserConfig


def test_factory():
    expected_names = sorted(
        ["NoopInputPathParser", "RegexInputPathParser", "ERA5LandInputPathParser", "ANINPathParser"]
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


class TestRegexInputPathParser:
    def test_construct_regexinputpathparser_from_config(self):
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

    def test_construct_regexinputpathparser_from_json(self):
        data = {
            "classname": "RegexInputPathParser",
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


class TestERA5LandInputPathParser:

    REGEX_PATTERN = ".*/reanalysis-era5-land-monthly-means_(?P<band>[a-zA-Z0-9\\_]+_monthly)_(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2})\\.tif$"

    @pytest.fixture
    def era5_path_parser(self) -> ERA5LandInputPathParser:
        return ERA5LandInputPathParser(regex_pattern=self.REGEX_PATTERN)

    @pytest.fixture
    def geotiff_paths(self) -> List[str]:
        return [
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20110101.tif",
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20110201.tif",
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20110301.tif",
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20110101.tif",
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20110201.tif",
            "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20110301.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20120101.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20120201.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20120301.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20120101.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20120201.tif",
            "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20120301.tif",
        ]

    def test_construct_from_json(self):
        data = {
            "classname": "ERA5LandInputPathParser",
            "parameters": {
                "regex_pattern": self.REGEX_PATTERN,
            },
        }
        config = InputPathParserConfig(**data)
        parser = InputPathParserFactory.from_config(config)
        assert isinstance(parser, ERA5LandInputPathParser)
        assert parser.regex.pattern == self.REGEX_PATTERN

    @pytest.mark.parametrize(
        ["path", "band", "year", "month", "day"],
        [
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20110101.tif",
                "2m_temperature_monthly",
                2011,
                1,
                1,
            ),
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20110201.tif",
                "2m_temperature_monthly",
                2011,
                2,
                1,
            ),
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20111201.tif",
                "2m_temperature_monthly",
                2011,
                12,
                1,
            ),
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20110101.tif",
                "total_precipitation_monthly",
                2011,
                1,
                1,
            ),
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20110201.tif",
                "total_precipitation_monthly",
                2011,
                2,
                1,
            ),
            (
                "/data/somewhere/2011/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20111201.tif",
                "total_precipitation_monthly",
                2011,
                12,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20120101.tif",
                "2m_temperature_monthly",
                2012,
                1,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20120201.tif",
                "2m_temperature_monthly",
                2012,
                2,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_2m_temperature_monthly_20121201.tif",
                "2m_temperature_monthly",
                2012,
                12,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20120101.tif",
                "total_precipitation_monthly",
                2012,
                1,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20120201.tif",
                "total_precipitation_monthly",
                2012,
                2,
                1,
            ),
            (
                "/data/somewhere/2012/reanalysis-era5-land-monthly-means_total_precipitation_monthly_20121201.tif",
                "total_precipitation_monthly",
                2012,
                12,
                1,
            ),
        ],
    )
    def test_it_parses_path_correctly(self, era5_path_parser: ERA5LandInputPathParser, path, band, year, month, day):
        parsed = era5_path_parser.parse(path)
        assert parsed["band"] == band
        assert parsed["year"] == year
        assert parsed["month"] == month
        assert parsed["day"] == day
