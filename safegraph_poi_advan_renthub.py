# Description: This script reads SafeGraph, Advan, and RentHub data
# Load them on maps using geopandas

import geopandas as gpd
from shapely import wkt

import webbrowser as wbr
import pandas as pd

# Load Safegraph POI data for Los Angeles, CA, US
safegraph_poi_sub = pd.read_csv (r'./data/safegraph_poi_sub_CA_Los_Angeles.csv')

# Convert to GeoDataFrame
# POLYGON_WKT string type to geometry
wkt.loads(safegraph_poi_sub['POLYGON_WKT'][0])

safegraph_poi_sub['POLYGON_WKT'] = safegraph_poi_sub['POLYGON_WKT'].apply(wkt.loads)
type(safegraph_poi_sub['POLYGON_WKT'][0])

safegraph_poi_gdf = gpd.GeoDataFrame(
    safegraph_poi_sub, geometry='POLYGON_WKT', crs="EPSG:4326"
)

# safegraph_poi_gdf.plot('WKT_AREA_SQ_METERS', legend=True)

safegraph_poi_map = safegraph_poi_gdf.explore("WKT_AREA_SQ_METERS", legend=True)
safegraph_poi_map.save("safegraph_poi_map.html")
wbr.open("safegraph_poi_map.html")

# 2. Advan monthly pattern data ----------------------

# Read advan_mp_data from the saved file
advan_mp_sub = pd.read_csv('./data/advan_mp_sub_CA_Los_Angeles_2024_Jan.csv')
print(advan_mp_sub['DATE_RANGE_START'].min())
print(advan_mp_sub['DATE_RANGE_START'].max())

advan_mp_sub = advan_mp_sub[advan_mp_sub['RAW_VISIT_COUNTS'].notna()]

# Only top 1000 RAW_VISIT_COUNTS
advan_mp_sub = advan_mp_sub.nlargest(1000, 'RAW_VISIT_COUNTS')
# Order by RAW_VISIT_COUNTS, descending
advan_mp_sub = advan_mp_sub.sort_values(by='RAW_VISIT_COUNTS', ascending=False)
advan_mp_sub.reset_index(drop=True, inplace=True)

advan_mp_sub['POLYGON_WKT'] = advan_mp_sub['POLYGON_WKT'].apply(wkt.loads)
type(advan_mp_sub['POLYGON_WKT'][0])

advan_mp_gdf = gpd.GeoDataFrame(
    advan_mp_sub, geometry='POLYGON_WKT', crs="EPSG:4326"
)

advan_mp_map = advan_mp_gdf.explore('RAW_VISIT_COUNTS', legend=True)
advan_mp_map.save("advan_mp_map.html")
wbr.open("advan_mp_map.html")

# 3. RentHub data --------------------

# Read renthub_data from the saved file
renthub_sub = pd.read_csv('./data/renthub_sub_CA_Los_Angeles_2023_Jul.csv')

# Summary of DATE_POSTED
print(renthub_sub['DATE_POSTED'].min())
print(renthub_sub['DATE_POSTED'].max())

# LATITUDE, LONGITUDE to geometry
# renthub_sub.loc[:, 'geometry'] = [Point(xy) for xy in zip(renthub_sub['LONGITUDE'],
#                                                    renthub_sub['LATITUDE'])]
# renthub_gdf = gpd.GeoDataFrame(renthub_sub, geometry='geometry', crs="EPSG:4326")
renthub_gdf = gpd.GeoDataFrame(renthub_sub, geometry=gpd.points_from_xy(renthub_sub['LONGITUDE'], renthub_sub['LATITUDE']))


# renthub_map = renthub_gdf.explore('RENT_PRICE', legend=True)
# Add RentHub layer to advan_mp_map
renthub_map = renthub_gdf.explore(m = advan_mp_map, column = 'RENT_PRICE', legend=True)

renthub_map.save("renthub_map.html")
wbr.open("renthub_map.html")
