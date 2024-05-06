import geopandas as gpd
from shapely import wkt

import webbrowser as wbr
import pandas as pd

import os

# 1. SafeGraph POI data on a map----------------------
# For CA, Los Angeles
# ----------------------------------------------------

# Your local folder here...
safegraph_folder = r"J:\Dewey\Data\SafeGraph\POI"

# List files in safegraph_folder
files = os.listdir(safegraph_folder)
safegraph_poi = pd.read_csv(os.path.join(safegraph_folder, files[0]))
# Only for Los Angels, CA, US
safegraph_poi_sub = safegraph_poi[(safegraph_poi['ISO_COUNTRY_CODE'] == 'US') &
                  (safegraph_poi['REGION'] == 'CA') &
                  (safegraph_poi['CITY'] == 'Los Angeles')]

# Remove if POLYGON_WKT is nan
safegraph_poi_sub = safegraph_poi_sub[safegraph_poi_sub['POLYGON_WKT'].notna()]
safegraph_poi_sub.reset_index(drop=True, inplace=True)

safegraph_poi_reserve_columns = ['PLACEKEY', 'PARENT_PLACEKEY', 'SAFEGRAPH_BRAND_IDS', 'LOCATION_NAME',
                   'BRANDS', 'STORE_ID', 'TOP_CATEGORY', 'SUB_CATEGORY', 'NAICS_CODE',
                   'LATITUDE', 'LONGITUDE', 'STREET_ADDRESS', 'CITY', 'REGION', 'POSTAL_CODE',
                   'GEOMETRY_TYPE', 'POLYGON_WKT', 'POLYGON_CLASS', 'ISO_COUNTRY_CODE', 'WKT_AREA_SQ_METERS']

# Save to local file
safegraph_poi_sub2 = safegraph_poi_sub[safegraph_poi_reserve_columns]
safegraph_poi_sub2.to_csv(r'./data/safegraph_poi_sub_CA_Los_Angeles.csv', index=False)

# Advan monthly pattern data ----------------------
# For CA, Los Angeles, January 2024 data.
# -------------------------------------------------

# Your local folder here...
advan_mp_folder = r'J:\Dewey\Data\Advan\MP_20240101'
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
advan_mp_reserve_columns = safegraph_poi_reserve_columns +\
                            ['DATE_RANGE_START', 'DATE_RANGE_END',
                             'RAW_VISIT_COUNTS', 'RAW_VISITOR_COUNTS', 'VISITS_BY_DAY', 'MEDIAN_DWELL']

advan_mp_sub = advan_mp_data[advan_mp_reserve_columns]
advan_mp_sub.to_csv('./data/advan_mp_sub_CA_Los_Angeles_2024_Jan.csv', index=False)

# 3. RentHub data ---------------------------
# For CA, Los Angeles, July 2023 data.
# -------------------------------------------

# Your local folder here...
renthub_folder = r'J:\Dewey\Data\RentHub\2023'

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
renthub_reserve_columns = ['ID', 'SCRAPED_TIMESTAMP', 'STATE', 'CITY', 'NEIGHBORHOOD', 'ZIP',
                            'ADDRESS', 'COMPANY', 'BUILDING_TYPE', 'BEDS', 'BATHS', 'SQFT', 'RENT_PRICE',
                            'DATE_POSTED', 'YEAR_BUILT', 'AVAILABLE_AT', 'AVAILABILITY_STATUS', 'LATITUDE', 'LONGITUDE']
renthub_data_sub = renthub_data[renthub_reserve_columns]
# DATE_POSTED only for 2023 July
renthub_data_sub = renthub_data_sub[renthub_data_sub['DATE_POSTED'].str.contains('2023-07', na=False)].copy()

renthub_data_sub.to_csv('./data/renthub_sub_CA_Los_Angeles_2023_Jul.csv', index=False)
