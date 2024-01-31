"""
Support for verifying that a new STAC collection/catalog works in openEO
"""

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


import openeo
from openeo.rest.datacube import DataCube
from openeo.util import rfc3339
from openeo.rest.job import BatchJob, JobFailedException


import pystac
from pystac import Collection, Item


DEFAULT_BACKEND = "openeo-dev.vito.be"


def connect(backend_url):
    connection: openeo.Connection = openeo.connect(backend_url)
    connection.authenticate_oidc()
    return connection


def get_first_item(collection: Collection) -> Item:
    items = collection.get_items()
    for item in items:
        break
    return item


def limit_spatial_extent_from_dict(
    extent: Dict[str, float],
    max_range: float,
) -> Dict[str, float]:
    new_west, new_south, new_east, new_north = limit_spatial_extent(*dict_to_bbox(extent), max_range=max_range)
    return bbox_to_dict(new_west, new_south, new_east, new_north)


def limit_spatial_extent(
    west: float, south: float, east: float, north: float, max_range: float, is_degrees: bool = False
):
    """Creates a new bounding box that is no larger than max_range, and with the same center as the original bounds."""
    range_x = abs(east - west)
    range_y = abs(north - south)
    avg_x = 0.5 * (west + east)
    avg_y = 0.5 * (south + north)

    new_range_x = min(max_range, range_x)
    new_range_y = min(max_range, range_y)

    new_west = avg_x - 0.5 * new_range_x
    new_east = avg_x + 0.5 * new_range_x
    if is_degrees:
        new_west = new_west % 360
        new_east = new_east % 360
        new_west = min(new_west, new_east)
        new_east = max(new_west, new_east)

    new_south = avg_y - 0.5 * new_range_y
    new_north = avg_y + 0.5 * new_range_y
    if is_degrees:
        new_south = new_south % 360
        new_north = new_north % 360
        new_south = min(new_south, new_north)
        new_north = max(new_south, new_north)

    return new_west, new_south, new_east, new_north


def bbox_to_dict(bbox: List[float]) -> Dict[str, float]:
    west, south, east, north = bbox[:4]
    return spatial_dict(west, south, east, north)


def dict_to_bbox(bbox_dict: Dict[str, float]) -> List[float]:
    b = bbox_dict
    return [b["west"], b["south"], b["east"], b["north"]]


def spatial_dict(west: float, south: float, east: float, north: float) -> Dict[str, float]:
    return {
        "west": west,
        "south": south,
        "east": east,
        "north": north,
    }


def find_spatial_extent(collection: Collection, max_spatial_ext_size: float = 0.1):
    first_item: Item = get_first_item(collection)
    base_extent = first_item.bbox[:4]
    return bbox_to_dict(limit_spatial_extent(*base_extent, max_range=max_spatial_ext_size))


def _dt_set_tz_utc(time: dt.datetime):
    return dt.datetime(
        time.year, time.month, time.day, time.hour, time.minute, time.second, time.microsecond, dt.timezone.utc
    )


def find_proj_bbox(collection: Collection) -> Tuple[Dict[str, Any], int]:
    first_item: Item = get_first_item(collection)
    epsg = 4326
    bbox = first_item.properties.get("proj:bbox")
    if bbox:
        epsg = first_item.properties.get("proj:epsg")
    else:
        bbox = first_item.geometry

    return bbox, epsg


def find_temporal_extent(collection: Collection, use_full: bool = False):
    base_extent: pystac.TemporalExtent = collection.extent.temporal
    start_dt, end_dt = base_extent.intervals[0]

    start_dt = _dt_set_tz_utc(start_dt)
    end_dt = _dt_set_tz_utc(end_dt)

    # TODO: This is not an accurate way to select a year, this won't give you calendar years
    # one_year = dt.timedelta(days=365)
    one_month = dt.timedelta(days=31)
    if not use_full and end_dt - start_dt > one_month:
        end_dt = start_dt + one_month

    start_dt = rfc3339.normalize(start_dt)
    end_dt = rfc3339.normalize(end_dt)

    return [start_dt, end_dt]


def create_cube_OLD(
    collection_path: Path,
    connection: openeo.Connection,
    bbox: Optional[Union[List[float], Dict[str, float]]] = None,
    epsg: Optional[int] = 4326,
    start_datetime: Optional[dt.datetime] = None,
    end_datetime: Optional[dt.datetime] = None,
    max_spatial_ext_size: float = None,
) -> None:
    collection = Collection.from_file(collection_path)

    if start_datetime or end_datetime:
        if not (start_datetime and end_datetime):
            raise ValueError(
                "If you want to specify a temporal extent then you must provide "
                + "a value for both start_datetime and end_datetime."
                + f"{start_datetime=}, {end_datetime=}"
            )
        extent_temporal = [start_datetime, end_datetime]
    else:
        extent_temporal = find_temporal_extent(collection, use_full=False)

    if bbox:
        proj_bbox = bbox
        proj_epsg = epsg
    else:
        proj_bbox, proj_epsg = find_proj_bbox(collection)

    print(f"{extent_temporal=}")
    print(f"{proj_bbox=}, {proj_epsg=}")

    cube: DataCube = connection.load_stac(
        str(collection_path),
        temporal_extent=extent_temporal,
    )

    if not bbox:
        proj_west, proj_south, proj_east, proj_north = proj_bbox[:4]
        west, south, east, north = limit_spatial_extent(
            proj_west, proj_south, proj_east, proj_north, max_range=max_spatial_ext_size
        )
        epsg = proj_epsg
    else:
        if isinstance(bbox, list):
            west, south, east, north = bbox[:4]
        else:
            west, south, east, north = dict_to_bbox(bbox)
    print(f"final spatial extent for filtering: {[west, south, east, north]}, {epsg=}")
    cube = cube.filter_bbox(west=west, south=south, east=east, north=north, crs=epsg)

    # print("cube.validate() ...")
    # validation_errors = cube.validate()
    # if validation_errors:
    #     print("Validation failed:")
    #     pprint(validation_errors)
    #     raise Exception("Validation failed")

    return cube


def create_cube(
    collection_path: Path,
    connection: openeo.Connection,
    extent_temporal,
    west: float,
    south: float,
    east: float,
    north: float,
    epsg: int,
) -> None:
    print("===create_cube:: Most important arguments: ===")
    print(f"{extent_temporal=}")
    print(f"{west=}")
    print(f"{south=}")
    print(f"{east=}")
    print(f"{north=}")
    print(f"{epsg=}")
    print("=== --- ===\n")

    cube: DataCube = connection.load_stac(
        str(collection_path),
        temporal_extent=extent_temporal,
    )

    # if isinstance(bbox, list):
    #     west, south, east, north = bbox[:4]
    # else:
    #     west, south, east, north = dict_to_bbox(bbox)

    print(f"final spatial extent for filtering: {[west, south, east, north]}, {epsg=}")
    cube = cube.filter_bbox(west=west, south=south, east=east, north=north, crs=epsg)

    # print("cube.validate() ...")
    # validation_errors = cube.validate()
    # if validation_errors:
    #     print("Validation failed:")
    #     pprint(validation_errors)
    #     raise Exception("Validation failed")

    return cube


def verify_in_openeo(
    collection_path: Union[str, Path],
    output_dir: Union[str, Path],
    backend_url: Optional[str] = None,
    max_spatial_ext_size: float = None,
    bbox: Optional[Union[List[float], Dict[str, float]]] = None,
    epsg: Optional[int] = 4326,
    start_datetime: Optional[dt.datetime] = None,
    end_datetime: Optional[dt.datetime] = None,
    dry_run: Optional[bool] = False,
    verbose: Optional[bool] = False,
):
    if dry_run:
        verbose = True

    backend_url = backend_url or DEFAULT_BACKEND

    print("===verify_in_openeo:: Most important arguments: ===")
    print(f"{max_spatial_ext_size=}")
    print(f"{bbox=}")
    print(f"{epsg=}")
    print(f"{start_datetime=}")
    print(f"{end_datetime=}")
    print("=== --- ===\n")

    coll_path = Path(collection_path)
    coll_dir: Path = coll_path.parent
    print(f"PROGRESS: Counting files in collection directory: {coll_dir}")
    print("    This may take long if there are very many files.")
    print(
        "    If this already takes long, then you should definitely LIMIT your spatial and temporal extent to keep the test on openEO short enough"
    )
    print("    Counting in progress ...")
    # files_in_collection = [f for f in coll_dir.rglob("*") if f.is_file()]
    # num_files = len(files_in_collection)
    num_files = 100_000
    print(f"DONE counting: Found {num_files} files.")

    extent_temporal = None
    proj_bbox = None
    proj_epsg = None
    print(f"PROGRESS: Reading the collection from file: {collection_path}")
    print(
        "    If this step takes too long that means your collection contains too many items and it needs to be divided up in to a catalog with smaller collections."
    )
    print("    Loading collection in progress: ...")
    collection = Collection.from_file(collection_path)

    if start_datetime or end_datetime:
        if not (start_datetime and end_datetime):
            raise ValueError(
                "If you want to specify a temporal extent then you must provide "
                + "a value for both start_datetime and end_datetime."
                + f"{start_datetime=}, {end_datetime=}"
            )
        extent_temporal = [start_datetime, end_datetime]
    else:
        extent_temporal = find_temporal_extent(collection, use_full=False)
    print(f"{extent_temporal=}")

    if bbox:
        proj_epsg = epsg
        if isinstance(bbox, list):
            west, south, east, north = bbox[:4]
        else:
            west, south, east, north = dict_to_bbox(bbox)
    else:
        proj_bbox, proj_epsg = find_proj_bbox(collection)
        west, south, east, north = proj_bbox[:4]

    if max_spatial_ext_size:
        west, south, east, north = limit_spatial_extent(west, south, east, north, max_range=max_spatial_ext_size)
    print(f"final spatial extent for filtering: {[west, south, east, north]}, {proj_epsg=}")

    abort = False
    if num_files > 1000:
        if not extent_temporal:
            abort = True
        else:
            dt_start, dt_end = extent_temporal
            one_month = dt.timedelta(days=31)
            if dt_end - dt_start > one_month:
                abort = True

    if abort:
        print(
            "This STAC collection has a large number of items and the "
            + "spatial/temporal extents are probably too large for a reasonable test\n."
        )
        print("ABORTED")
        return

    intervals = collection.extent.temporal.intervals
    if len(intervals) == 1:
        coll_start, coll_end = intervals[0]
        print(f"{coll_start=}, {coll_end=}")
        print(f"{dt_start=}, {dt_end=}")

        if dt_start < coll_start:
            raise ValueError(
                "Start datetime is before the start of the collection: dt_start < coll_start"
                + f"{dt_start=}, {coll_start=}"
            )
        if dt_end > coll_end:
            raise ValueError(
                f"End datetime is after the start of the collection: dt_end > coll_end, {dt_end=}, {coll_end=}"
            )

    connection = connect(backend_url)

    output_dir = Path(output_dir)
    job_log_file = output_dir / "job-logs.json"

    timestamp = rfc3339.utcnow()
    timestamp = timestamp.replace(":", "")

    collection_path = Path(collection_path).expanduser().absolute()
    if verbose:
        print(f"Collection's absolute path: {collection_path}")
        print(f"Does collection file exist? {collection_path.exists()}")
        print(f"{output_dir=}")

    assert collection_path.exists(), f"file should exist: {collection_path=}"
    # print(f"Validating STAC collection file: {collection_path} ...")
    # Collection.from_file(collection_path).validate_all()

    if not output_dir.exists() and not dry_run:
        print(f"Creating output_dir: {output_dir}")
        output_dir.mkdir(parents=True)

    print("PROGRESS: Creating DataCube: ...")
    cube: DataCube = create_cube(
        collection_path=str(collection_path),
        connection=connection,
        extent_temporal=extent_temporal,
        west=west,
        south=south,
        east=east,
        north=north,
        epsg=proj_epsg,
    )
    print(cube)

    # print("PROGRESS: Validating DataCube ...")
    # cube.validate()

    if dry_run:
        print("DONE: This is a dry run. Skipping part that submits a batch job")
        return

    job: BatchJob = cube.create_job()
    try:
        job.start_and_wait()
    except JobFailedException as exc:
        print(exc)
        get_logs(job, job_log_file)

    else:
        print(job.get_results_metadata_url())

        out_path = job.download_results(output_dir)
        print(f"{out_path=}")
        get_logs(job, job_log_file)

    print("DONE")


def check_job(
    job_id: str,
    output_dir: Union[str, Path],
    backend_url: Optional[str] = None,
) -> None:
    backend_url = backend_url or DEFAULT_BACKEND

    connection = connect(backend_url)

    output_dir = Path(output_dir)
    job_log_file = output_dir / "job-logs.json"

    if job_id:
        job = connection.job(job_id=job_id)
        print(f"=== job_id: {job_id}")
        print(f"Job status: {job.status()}")
        get_logs(job, job_log_file)

        if job.status() == "finished":
            out_path = job.download_results(output_dir)
            print(f"{out_path=}")


def get_logs(job: BatchJob, job_log_file: Optional[Path] = None) -> None:
    # print("=== logs ===")
    # for record in job.logs():
    #     print(record)
    # print("=== === ===")

    if job_log_file:
        with open(job_log_file, "wt", encoding="utf8") as f_log:
            json.dump(job.logs(), f_log, indent=2)
