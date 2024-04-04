# Description: This script reads SafeGraph, Advan, and RentHub data
# Load them on maps using geopanda

import geopandas as gpd
from shapely import wkt

import webbrowser as wbr
import os
import pandas as pd

# 1. SafeGraph POI data on a map----------------------
# Your local folder here...
sf_folder = r"J:\Dewey\Data\SafeGraph"

# List files in sf_folder
files = os.listdir(sf_folder)
sg_data = pd.read_csv(os.path.join(sf_folder, files[0]))
# only for Los Angels, CA, US
sg_data_sub = sg_data[(sg_data['ISO_COUNTRY_CODE'] == 'US') &
                  (sg_data['REGION'] == 'CA') &
                  (sg_data['CITY'] == 'Los Angeles')]

# Remove if POLYGON_WKT is nan
sg_data_sub = sg_data_sub[sg_data_sub['POLYGON_WKT'].notna()]
sg_data_sub.reset_index(drop=True, inplace=True)

# Convert to GeoDataFrame
# POLYGON_WKT string type to geometry
wkt.loads(sg_data_sub['POLYGON_WKT'][0])

sg_data_sub['POLYGON_WKT'] = sg_data_sub['POLYGON_WKT'].apply(wkt.loads)
type(sg_data_sub['POLYGON_WKT'][0])

sg_data_gdf = gpd.GeoDataFrame(
    sg_data_sub, geometry='POLYGON_WKT', crs="EPSG:4326"
)

# sg_data_gdf.plot('WKT_AREA_SQ_METERS', legend=True)

sg_data_map = sg_data_gdf.explore("WKT_AREA_SQ_METERS", legend=True)
sg_data_map.save("sg_data_map.html")
wbr.open("sg_data_map.html")

# 2. Advan monthly pattern data ----------------------
# Your local folder here...
advan_mp_read_from_file = False
advan_mp_folder = r'J:\Dewey\Data\Advan\MP_20240101'
advan_mp_data_save_file_path = './data/Advan_MP_Los_Angeles_20240101.csv'

if advan_mp_read_from_file:
    files = os.listdir(advan_mp_folder)
    # Read all the files in the folder and filter only for
    # 'ISO_COUNTRY_CODE' == 'US') &
    # 'REGION' == 'CA' &
    # 'CITY' == 'Los Angeles'
    # and merge into one dataframe
    advan_mp_data = None
    for file in files:
        print(f'Processing {file}...')
        advan_mp1 = pd.read_csv(os.path.join(advan_mp_folder, file))
        advan_mp1 = advan_mp1[advan_mp1['POLYGON_WKT'].notna()]
        advan_mp1 = advan_mp1[(advan_mp1['ISO_COUNTRY_CODE'] == 'US') &
                              (advan_mp1['REGION'] == 'CA') &
                              (advan_mp1['CITY'] == 'Los Angeles')]
        # merge
        advan_mp_data = pd.concat([advan_mp_data, advan_mp1])

    advan_mp_data.reset_index(drop=True, inplace=True)

    # Save the merged dataframe
    advan_mp_data_file_path = './data/Advan_MP_Los_Angeles_20240101.csv'
    advan_mp_data.to_csv(advan_mp_data_save_file_path, index=False)

# Read advan_mp_data from the saved file
advan_mp_data = pd.read_csv(advan_mp_data_save_file_path)

# Only coulumns 'RAW_VISIT_COUNTS', 'POLYGON_WKT'
advan_mp_sub = advan_mp_data[['PLACEKEY', 'RAW_VISIT_COUNTS', 'RAW_VISITOR_COUNTS',
                             'LOCATION_NAME', 'SUB_CATEGORY',
                             'ISO_COUNTRY_CODE', 'REGION', 'CITY', 'POSTAL_CODE',
                             'POLYGON_WKT', 'LATITUDE', 'LONGITUDE', 'WKT_AREA_SQ_METERS']]

advan_mp_sub = advan_mp_data[advan_mp_data['RAW_VISIT_COUNTS'].notna()]

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
renthub_read_from_file = False
# Your local folder here...
renthub_folder = r'J:\Dewey\Data\RentHub\2023'
renthub_data_save_file_path = './data/RentHub_Los_Angeles_2023.csv'

if renthub_read_from_file:
    # Read all the files in the folder and filter only for
    # 'STATE' == 'CA' &
    # 'CITY' == 'Los Angeles'
    # and merge into one dataframe

    files = os.listdir(renthub_folder)
    renthub_data = None
    for file in files:
        print(f'Processing {file}...')
        renthub1 = pd.read_csv(os.path.join(renthub_folder, file))
        renthub1 = renthub1[(renthub1['STATE'] == 'CA') &
                            (renthub1['CITY'] == 'Los Angeles')]
        # merge
        renthub_data = pd.concat([renthub_data, renthub1])

    # Save the merged dataframe
    renthub_data.to_csv(renthub_data_save_file_path, index=False)

# Read renthub_data from the saved file
renthub_data = pd.read_csv(renthub_data_save_file_path)

# DATE_POSTED only for 2023 July
renthub_sub = renthub_data[renthub_data['DATE_POSTED'].str.contains('2023-07', na=False)].copy()
# Excluding the DESCRIPTION column
renthub_sub = renthub_sub.drop(columns=['DESCRIPTION'])

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
