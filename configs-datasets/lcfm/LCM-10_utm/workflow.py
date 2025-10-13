<<<<<<< HEAD
from time import sleep
import pystac
from upath import UPath
from pathlib import Path
import pprint
from getpass import getpass

=======
import pprint
from pathlib import Path

import pystac
>>>>>>> origin/main
from shapely.geometry import LineString, MultiPolygon, Polygon
from shapely.ops import split

# run pip install -e . in the root directory to install this package
<<<<<<< HEAD
from stacbuilder import *
=======
from stacbuilder import (
    build_collection,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    validate_collection,
)
>>>>>>> origin/main

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/vitodata/vegteam_lcfm_openeo/LCFM/LCM-10/v008/tiles_utm/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/*/2020/*_MAP.tif"


def intersects_antimeridian(polygon: Polygon, lon_threshold: int = 100) -> bool:
    """
    Check if a polygon intersects the antimeridian (longitude 180/-180).
    This is done by checking if the polygon crosses the antimeridian by looking for a large jump in
    longitude between consecutive points. This is used for a grid of cells which
    are not expected to extend from -lon_threshold to lon_threshold, meaning that
    if this is the case, the polygon crosses the antimeridian and should be split
    accordingly.
    """
    coords = list(polygon.exterior.coords)
    crosses_antimeridian = any(
        (x1 > lon_threshold and x2 < -lon_threshold) or (x1 < -lon_threshold and x2 > lon_threshold)
        for (x1, y1), (x2, y2) in zip(coords, coords[1:] + [coords[0]])
    )
    return crosses_antimeridian


def _fix_antimeridian_split(polygon: Polygon) -> MultiPolygon:
    """
    Split a polygon that crosses the antimeridian (longitude 180/-180) into two parts.
    This is done by adding a line that crosses the antimeridian and using it to split the polygon.
    """

    eps = 0
    splitter = LineString([(-180, 90), (-180, -90)])
    coords = list(polygon.exterior.coords)

    # Normalize coordinates to split across the antimeridian
    normalized_coords = [(x - 360 if x > 0 else x, y) for x, y in coords]
    normalized_polygon = Polygon(normalized_coords)

    # Split the polygon using the splitter line
    split_result = split(normalized_polygon, splitter)

    # # Filter out non-polygon geometries (like LinearRings or Points that might be created)
    # split_polygons = [geom for geom in split_result if isinstance(geom, Polygon)]

    # Normalize the polygons to have their coordinates within the range (-180, 180)
    fixed_polygons = []
    for poly in split_result.geoms:
        new_coords = []
        if poly.centroid.x < -180:
            new_coords = [(x + 360, y) for x, y in poly.exterior.coords]
            new_coords = [(x + eps, y) if x == -180 else (x, y) for x, y in new_coords]
            new_coords = [(x - eps, y) if x == 180 else (x, y) for x, y in new_coords]
            fixed_polygons.append(Polygon(new_coords))
        else:
            new_coords = [(x, y) for x, y in poly.exterior.coords]
            new_coords = [(x + eps, y) if x == -180 else (x, y) for x, y in new_coords]
            new_coords = [(x - eps, y) if x == 180 else (x, y) for x, y in new_coords]
            fixed_polygons.append(Polygon(new_coords))
            # fixed_polygons.append(poly)

    # Return the resulting polygons as a MultiPolygon
    new_polygon = MultiPolygon(fixed_polygons)

    return new_polygon


def fix_antimeridian_split(polygon: Polygon, lon_threshold: int = 100) -> Polygon | MultiPolygon:
    # A line that we will use to split the polygon at the antimeridian
    if intersects_antimeridian(polygon, lon_threshold):
        return _fix_antimeridian_split(polygon)
    else:
        return polygon  # Return the original polygon if it doesn't cross the antimeridian


def _slash_tile(tile: str):
    if len(tile) == 3:
        return f"{tile[:2]}/{tile[2:]}/"
    elif len(tile) == 5:
        return f"{tile[:2]}/{tile[2]}/{tile[3:]}"

    raise ValueError(f"tile should be a str of len 3 or 5, not {tile}")


# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
<<<<<<< HEAD
overwrite = True
=======
>>>>>>> origin/main


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=10)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)


# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


def item_postprocessor(item: pystac.Item) -> pystac.Item:
    # item.properties["proj:code"] = "EPSG:" + str(item.properties["proj:epsg"])
    # del item.properties["proj:epsg"]
    # item.stac_extensions[2] = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"

    tile = item.properties["product_tile"]
    item.properties["tileId"] = tile
    del item.properties["product_tile"]

    if tile.startswith("01") or tile.startswith("60"):
        polygon = Polygon(item.geometry["coordinates"][0])
        polygon = fix_antimeridian_split(polygon)
        if polygon is MultiPolygon:
            item.geometry["coordinates"] = [
                polygon.__geo_interface__["coordinates"][0][0],
                polygon.__geo_interface__["coordinates"][1][0],
            ]
            if tile.startswith("01"):
                geom_index = 0 if polygon.geoms[0].bounds[0] < 0 else 1
            else:
                geom_index = 0 if polygon.geoms[0].bounds[0] > 0 else 1
            item.bbox = tuple(polygon.geoms[geom_index].bounds)

    s3_bucket = "s3://vito-upload"
    s3_prefix = f"LCM-10/v008/tiles_utm/{_slash_tile(tile)}"
    s3_postfix = "/".join(item.assets["map"].href.split("/")[-2:-1])
    s3_name = item.assets["map"].href.split("/")[-1]
    item.assets["map"].href = s3_bucket + "/" + s3_prefix + "/" + s3_postfix + "/" + s3_name
    s3_bucket = "s3://lcfm_waw3-1_4b82fdbbe2580bdfc4f595824922507c0d7cae2541c0799982/vito/validation"
    item.assets["map"].extra_fields.setdefault("alternate", {}).setdefault("local", {})["href"] = (
        s3_bucket + "/" + s3_prefix + "/" + s3_postfix + "/" + s3_name
    )
    return item


# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1,
    item_postprocessor=item_postprocessor,
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")

print("First stac item:")
pprint.pprint(stac_items[0].to_dict())

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=test_output_path,
    link_items=False,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file=test_output_path / "collection.json",
)
