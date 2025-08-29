import openeo
from shapely.geometry import shape

c = openeo.connect("https://openeo.vito.be").authenticate_oidc()


glacier_area = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "coordinates": [
                    [
                        [-70.13104066031413, -33.000276542916495],
                        [-70.23738638736786, -33.13188389978371],
                        [-70.05674617853818, -33.561422495664495],
                        [-69.72772286706841, -33.5698315816253],
                        [-69.86563069608333, -32.906466815548875],
                        [-70.01360153733002, -32.893889358907465],
                        [-70.13104066031413, -33.000276542916495],
                    ]
                ],
                "type": "Polygon",
            },
        }
    ],
}


aoi = shape(glacier_area["features"][0]["geometry"])

cube = c.load_stac("https://stac.openeo.vito.be/collections/modis-09GA-061", temporal_extent="2018-01").filter_spatial(
    aoi
)

result = cube.validate()
print(result)
cube.execute_batch("modis09.nc")
