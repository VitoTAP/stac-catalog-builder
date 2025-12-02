import math
from pathlib import Path

import requests
import shapely
from pystac import Item, Collection, Provider, ProviderRole
import pystac
import pystac.extensions.raster
import xarray
from shapely.geometry import box

from stacbuilder import CollectionConfig
from stacbuilder.config import ProviderModel
from stacbuilder.item_pipeline import ItemMetadataPipeline
from stacbuilder.stac_item_collector import STACItemCollector

from eodag import EODataAccessGateway


eodag_types = {
    'SWE_MERGED_V4.0': {'application/x-netcdf'},
    #'AATSR_ADV_L2_V2.31': {'application/x-netcdf'},
 #'AATSR_ADV_L3_V2.31': {'application/x-netcdf'},
 # 'AATSR_ENS_L2_V2.6': {'application/x-netcdf'},
 # 'AATSR_ENS_L3_V2.6': {'application/x-netcdf'},
 # 'AATSR_ORAC_L2_V4.01': {'application/x-netcdf'},
 # 'AATSR_ORAC_L3_V4.01': {'application/x-netcdf'},
 # 'AATSR_SU_L2_V4.3': {'application/x-netcdf'},
 # 'AATSR_SU_L3_V4.3': {'application/x-netcdf'},
#'AQUA_MODIS_L3C_0.01_V3.00_DAILY': {'application/x-netcdf'},
  'AQUA_MODIS_L3C_0.01_V3.00_MONTHLY': {'application/x-netcdf'}, #WORKS!!
# 'ARCTIC_MSLA_20161024': {'application/x-netcdf'}, #BAD collection
#'BURNED_AREA_MODIS_GRID_V5.1': {'application/x-netcdf'},
  #  'CCI_PLUS_CH4_GO2_SRFP_V2.0.3': {'application/x-netcdf'}, #No usable vars?
  'CDR_V2_ANALYSIS_L4_V2.1': {'application/x-netcdf'}, #WORKS!!
#'LAKE_PRODUCTS_L3S_V2.1': {'application/x-netcdf'}, #mixed items
    'SCFV_MODIS_V3.0': {'application/x-netcdf'}, # WORKS!!
    'V6.0-RELEASE_GEOGRAPHIC_NETCDF_RRS': {'application/x-netcdf'},
 }

gateway = EODataAccessGateway()
product_types = gateway.list_product_types(provider="fedeo_ceda")
product_config = gateway.providers_config["fedeo_ceda"].products



def degrees_to_meters(xRes, latitude=0):
    """
    Convert resolution in degrees to approximate resolution in meters.

    :param xRes: Resolution in degrees (longitude).
    :param latitude: Latitude in degrees.
    :return: Resolution in meters.
    """
    # Earth's radius in meters
    earth_radius = 6371000

    # Convert latitude to radians
    latitude_rad = math.radians(latitude)

    # Convert xRes to meters
    meters = xRes * (math.pi / 180) * earth_radius * math.cos(latitude_rad)

    return meters

for name in eodag_types.keys():

    c = product_config[name]['productType']
    collection_url = f"https://fedeo.ceos.org/collections/{c}"

    stac_collection = Collection.from_file(collection_url)

    providers = [ ProviderModel(** p.to_dict()) for p in stac_collection.providers]

    config = CollectionConfig(collection_id= name, title= product_config[name]['title'], description=stac_collection.description,providers = providers)

    def postprocess(item):
        print(item)



        netcdf_assets = {
            k: a
            for k,a in item.assets.items()
            if a.media_type == "application/x-netcdf"
        }
        union_geometry: shapely.Geometry = None
        for asset_name, data_asset in netcdf_assets.items():

            asset_url = data_asset.href
            try:
                dataset =  xarray.open_dataset(asset_url)

                doi = dataset.attrs.get("doi", None)
                if doi is not None:
                    item.extra_fields["sci:doi"] = doi
                website = dataset.attrs.get("references", None)
                if website is not None:
                    link = pystac.Link(rel="about", target=website, title="Dataset Website")
                    if link not in item.links:
                        item.links.append(link)
                for v in dataset.variables.values():

                    print(v.attrs)
                    print(v.shape)
                    print(v.dims)
                raster_variables = { name: var for name, var in dataset.variables.items() if len(var.shape) >= 2 and ("grid_mapping" in var.attrs or var.dims == ("time", "lat", "lon"))}
                if 'creator_name' in dataset.attrs:
                    providers = item.common_metadata.providers or []

                    provider = Provider(name=dataset.attrs.get("creator_name", ""),
                                        url=dataset.attrs.get("creator_url", ""), roles=[ProviderRole.PRODUCER])
                    if provider not in providers:
                        providers.append(provider)
                    item.common_metadata.providers = providers
                def get_band(name,var):
                    datatype = str(var.dtype)
                    if datatype.upper() not in pystac.extensions.raster.DataType.__members__:
                        datatype = pystac.extensions.raster.DataType.OTHER

                    band = dict(name=name, description=var.attrs.get("long_name", None), data_type=datatype)
                    nonlocal union_geometry


                    if "flag_meanings" in var.attrs and "no_data" in var.attrs.get("flag_meanings", ""):
                        nodata_index = var.attrs.get("flag_meanings", "").split(" ").index("no_data")
                        if(nodata_index>=0 and "flag_values" in var.attrs):
                            band["nodata"] = float(var.attrs.get("flag_values", [])[nodata_index])

                    if( var.dims == ("time", "lat", "lon")):
                        band["proj:shape"] = list(var.shape)[1:]
                        lat = dataset.variables["lat"]
                        minlat = lat.values.min()
                        maxlat = lat.values.max()
                        yRes = (maxlat - minlat) / len(lat.values)

                        lon = dataset.variables["lon"]
                        minlon = lon.values.min()
                        maxlon = lon.values.max()
                        xRes = (maxlon - minlon) / len(lon.values)
                        precision = 6
                        bbox = [round(float(minlon - xRes / 2.0), precision), round(float(minlat - yRes / 2.0), precision), round(float(maxlon + xRes / 2.0), precision),
                                round(float(maxlat + xRes / 2.0), precision)]
                        band["proj:bbox"] = bbox
                        union_geometry = box(*bbox) if union_geometry == None else union_geometry.union(box(*bbox))
                        band["proj:code"] = 4326
                        band["raster:spatial_resolution"] = round(degrees_to_meters(float(xRes)),1)
                        if "units" in var.attrs:
                            band["unit"] = var.attrs.get("units", None)


                    return band


                bands = [ get_band(name,var) for name,var in  raster_variables.items() ]
                data_asset.extra_fields["bands"] = bands
            except Exception as e:
                print(e)

        if union_geometry is not None:
            item.geometry = shapely.geometry.mapping(union_geometry)
            item.bbox = list(union_geometry.bounds)

        if "enclosure_1" in item.assets and "enclosure_2" in item.assets:
            assert item.assets["enclosure_1"].title == "Download"
            assert item.assets["enclosure_2"].title == "Opendap"
            item.assets["enclosure_2"].extra_fields["alternate"] = {
                "Opendap": {
                    "href": item.assets["enclosure_2"].href,
                    "alternate:name": "Opendap",
                }
            }
            item.assets["enclosure_2"].href = item.assets["enclosure_1"].href
            item.assets["enclosure_2"].title = "Download" #better asset title?
            item.assets.pop("enclosure_1")

        return item

    pipeline = ItemMetadataPipeline(STACItemCollector(collection_url, 100), config, Path(f"{name}"), item_postprocessor=postprocess)

    pipeline.build_collection()