import datetime as dt
import json
from pprint import pprint
from pathlib import Path
from typing import Dict, List, Optional, Union
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


# def find_spatial_extent(collection: Collection):
#     base_extent = collection.extent.spatial
#     ext_list = base_extent.to_dict()["bbox"][0]
#     west, south, east, north = ext_list

#     range_x = abs(west - east)
#     range_y = abs(north - south)
#     avg_x = east + 0.5 * range_x
#     avg_y = south + 0.5 * range_y

#     # limit the bbox to 0.01 degrees
#     max_range = 0.01
#     new_range_x = min(max_range, range_x)
#     new_range_y = min(max_range, range_y)

#     new_west = (avg_x - 0.5 * new_range_x) % 360
#     new_east = (avg_x + 0.5 * new_range_x) % 360
#     new_west = min(new_west, new_east)
#     new_east = max(new_west, new_east)

#     new_south = (avg_y - 0.5 * new_range_y) % 360
#     new_north = (avg_y + 0.5 * new_range_y) % 360
#     new_south = min(new_south, new_north)
#     new_north = max(new_south, new_north)

#     return {"east": new_east, "south": new_south, "west": new_west, "north": new_north}


def bbox_to_dict(bbox: List[float]) -> Dict[str, float]:
    west, south, east, north = bbox[:4]
    return spatial_dict(west, south, east, north)


def spatial_dict(
        west: float, south: float, east: float, north: float
    ) -> Dict[str, float]:
    return {
        "west": west, "south": south,
        "east": east, "north": north,
    }


def find_spatial_extent(collection: Collection):
    first_item: Item = get_first_item(collection)
    return bbox_to_dict(first_item.bbox)


def _dt_set_tz_utc(time: dt.datetime):
    return dt.datetime(
        time.year, time.month, time.day, time.hour, time.minute, time.second, time.microsecond, dt.timezone.utc
    )


def find_temporal_extent(collection: Collection):
    base_extent: pystac.TemporalExtent = collection.extent.temporal
    start_dt, end_dt = base_extent.intervals[0]

    start_dt = _dt_set_tz_utc(start_dt)
    end_dt = _dt_set_tz_utc(end_dt)
    start_dt = rfc3339.normalize(start_dt)
    end_dt = rfc3339.normalize(end_dt)

    # start_dt = None
    # end_dt = None
    # if "start_datetime" in first_item.properties:
    #     start_dt = first_item.properties["start_datetime"]
    #     if "end_datetime" in first_item.properties:
    #         end_dt = first_item.properties["end_datetime"]
    # else:
    #     start_dt = first_item.get_datetime()

    # if not end_dt:
    #     end_dt = start_dt + dt.timedelta(days=1)

    return [start_dt, end_dt]


def create_cube(collection_path: Path, connection: openeo.Connection):
    collection = Collection.from_file(collection_path)
    extent_spatial = find_spatial_extent(collection)
    extent_temporal = find_temporal_extent(collection)
    print(f"{extent_spatial=}")
    print(f"{extent_temporal=}")

    cube = connection.load_stac(
        str(collection_path),
        spatial_extent=extent_spatial,
        temporal_extent=extent_temporal,
    )

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
    dry_run: bool = False,
    verbose: bool = False,
):
    if dry_run:
        verbose = True
    connection = connect(backend_url)

    # job_id = "j-231206b597464ad491cad2e902971ff7"
    # job_id = "j-231211402f5843ab92a2a557c8cfd2cf"
    job_id = None

    output_dir = Path(output_dir)
    job_log_file = output_dir / "job-logs.json"
   
    if job_id:
        job = connection.job(job_id=job_id)
        get_logs(job, job_log_file)

    else:
        timestamp = rfc3339.utcnow()
        timestamp = timestamp.replace(":", "")

        collection_path = Path(collection_path).expanduser().absolute()
        if verbose:
            print(f"Collection's absolute path: {collection_path}")
            print(f"Does collection file exist? {collection_path.exists()}")
            print(f"{output_dir=}")

        assert collection_path.exists(), f"file should exist: {collection_path=}"
        Collection.from_file(collection_path).validate_all()

        if not output_dir.exists() and not dry_run:
            print(f"Creating output_dir: {output_dir}")
            output_dir.mkdir(parents=True)

        cube: DataCube = create_cube(str(collection_path), connection)
        print(cube)
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
    print("=== logs ===")
    for record in job.logs():
        print(record)
    print("=== === ===")

    if job_log_file:
        with open(job_log_file, "wt", encoding="utf8") as f_log:
            json.dump(job.logs(), f_log, indent=2)
