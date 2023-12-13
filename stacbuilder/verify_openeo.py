import datetime as dt
import json
from pprint import pprint
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from itertools import islice


import openeo
from openeo.rest.datacube import DataCube
from openeo.util import rfc3339
from openeo.rest.job import BatchJob, JobFailedException


import pystac
from pystac import Collection, Item


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
    one_year = dt.timedelta(days=365)
    if not use_full and end_dt - start_dt > one_year:
        end_dt = start_dt + one_year

    start_dt = rfc3339.normalize(start_dt)
    end_dt = rfc3339.normalize(end_dt)

    return [start_dt, end_dt]


def create_cube(
    collection_path: Path,
    connection: openeo.Connection,
    bbox: Optional[Union[List[float], Dict[str, float]]] = None,
    epsg: Optional[int] = 4326,
    max_spatial_ext_size: float = None,
) -> None:
    collection = Collection.from_file(collection_path)
    extent_temporal = find_temporal_extent(collection, use_full=True)
    print(f"{extent_temporal=}")

    proj_bbox, proj_epsg = find_proj_bbox(collection)
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

    print("cube.validate() ...")
    validation_errors = cube.validate()
    if validation_errors:
        print("Validation failed:")
        pprint(validation_errors)
        raise Exception("Validation failed")

    return cube


def verify_in_openeo(
    collection_path: Union[str, Path],
    output_dir: Union[str, Path],
    backend_url: str = "openeo-dev.vito.be",
    max_spatial_ext_size: float = 0.1,
    bbox: Optional[Union[List[float], Dict[str, float]]] = None,
    epsg: Optional[int] = 4326,
    dry_run: Optional[bool] = False,
    verbose: Optional[bool] = False,
):
    if dry_run:
        verbose = True
    connection = connect(backend_url)

    job_id = None
    # job_id = "j-231206b597464ad491cad2e902971ff7"
    # job_id = "j-231211402f5843ab92a2a557c8cfd2cf"
    # job_id = "j-231212dab20845b9a6570c2c1832102c"

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

    else:
        timestamp = rfc3339.utcnow()
        timestamp = timestamp.replace(":", "")

        collection_path = Path(collection_path).expanduser().absolute()
        if verbose:
            print(f"Collection's absolute path: {collection_path}")
            print(f"Does collection file exist? {collection_path.exists()}")
            print(f"{output_dir=}")

        assert collection_path.exists(), f"file should exist: {collection_path=}"
        print(f"Validating STAC collection file: {collection_path} ...")
        Collection.from_file(collection_path).validate_all()

        if not output_dir.exists() and not dry_run:
            print(f"Creating output_dir: {output_dir}")
            output_dir.mkdir(parents=True)

        print(f"Creating DataCube: ...")
        cube: DataCube = create_cube(str(collection_path), connection, bbox, epsg, max_spatial_ext_size)
        print(cube)

        print(f"Validating DataCube ...")
        cube.validate()

        if dry_run:
            print("This is a dry run. Skipping part that submits a batch job")
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

        print("DONE")


def get_logs(job: BatchJob, job_log_file: Optional[Path] = None) -> None:
    # print("=== logs ===")
    # for record in job.logs():
    #     print(record)
    # print("=== === ===")

    if job_log_file:
        with open(job_log_file, "wt", encoding="utf8") as f_log:
            json.dump(job.logs(), f_log, indent=2)
