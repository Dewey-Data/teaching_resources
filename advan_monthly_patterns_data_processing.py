import pandas as pd
import os
import time
import pyarrow.csv as pv
import pyarrow as pa

local_dir = r'J:\Dewey\Data\Advan\MP_20240419'

process_raw_data = False
if process_raw_data:
    files = os.listdir(local_dir)

    # Read the first file
    # Compare reading time with engine='pyarrow' and without

    start_time = time.time()
    foot_traffic_sample1 = pd.read_csv(os.path.join(local_dir, files[0]))
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    # pd.read_csv(os.path.join(local_dir, files[0]), engine='pyarrow')
    foot_traffic_sample = pv.read_csv(os.path.join(local_dir, files[0]), read_options=pv.ReadOptions(block_size=32 * 1024 * 1024))
    foot_traffic_sample = foot_traffic_sample.to_pandas()
    print("--- %s seconds ---" % (time.time() - start_time))
    foot_traffic_sample.equals(foot_traffic_sample1)

    foot_traffic_sample = pd.read_csv(os.path.join(local_dir, files[0]))

    foot_traffic_sample = pv.read_csv(os.path.join(local_dir, files[0]), read_options=pv.ReadOptions(block_size=32 * 1024 * 1024))
    foot_traffic_sample = foot_traffic_sample.to_pandas()


    # Unique brand names
    # unique_brands = pd.DataFrame(spend_sample['BRANDS'].unique())
    unique_brands = foot_traffic_sample[['BRANDS', 'SAFEGRAPH_BRAND_IDS']].drop_duplicates()
    unique_brands.dropna(inplace = True)
    unique_brands.sort_values(by = ['BRANDS'], inplace = True)
    unique_brands[unique_brands['BRANDS'].str.contains('bank of america|chase|wells fargo|keller williams|remax|century 21', case=False)]
    brand_names = ['Bank of America', 'Chase', 'Wells Fargo', 'REMAX', 'Century 21 Real Estate', 'Keller Williams',
                   'Shell Oil', 'Chevron', 'Starbucks', 'Costco', 'Target', 'Walmart', 'The Home Depot']
    brand_names = ['Tesla Motors', 'Tesla Destination Charger', 'Tesla Supercharger', 'Tesla Service Center']

    select_brands = unique_brands[unique_brands['BRANDS'].isin(brand_names)]
    select_brands

    # Filter for the selected brands for all the files
    # List to store each chunk of data
    select_brand_ids = set(select_brands['SAFEGRAPH_BRAND_IDS'])  # Assuming select_brands is already a Pandas DataFrame or similar

    value_set=pa.array(list(select_brand_ids))
    read_options=pv.ReadOptions(block_size=32 * 1024 * 1024)
    data_chunks = []
    for file in files:
        path = os.path.join(local_dir, file)
        print(f'Processing {path}...')

        # Read the CSV file with PyArrow
        table = pv.read_csv(path, read_options = read_options)

        # Filter rows directly in PyArrow by creating a boolean mask and filtering the table
        # mask = pa.compute.is_in(table['SAFEGRAPH_BRAND_IDS'], value_set=pa.array(list(select_brand_ids)))
        mask = pa.compute.is_in(table['SAFEGRAPH_BRAND_IDS'], value_set=value_set)
        filtered_table = table.filter(mask)
        # filtered_table = filtered_table.to_pandas()

        data_chunks.append(filtered_table)

    def adjust_schema(table):
        schema = table.schema
        new_fields = []
        for field in schema:
            # Ensure STORE_ID is int64
            if field.name == 'STORE_ID' and field.type != pa.int64():
                new_field = pa.field('STORE_ID', pa.int64())
            # Ensure CLOSED_ON, OPENED_ON, TRACKING_CLOSED_SINCE are nullable timestamps
            elif field.name in ['CLOSED_ON', 'OPENED_ON', 'TRACKING_CLOSED_SINCE'] and field.type != pa.timestamp('ns'):
                new_field = pa.field(field.name, pa.timestamp('ns'), nullable=True)
            else:
                new_field = field
            new_fields.append(new_field)
        return table.cast(pa.schema(new_fields))

    # Assume data_chunks is a list of your tables
    adjusted_tables = [adjust_schema(table) for table in data_chunks]

    # Concatenate tables with promotion of compatible types
    advan_mp_sample_data = pa.concat_tables(adjusted_tables, promote_options='default')

    # sort_indices = pa.compute.sort_indices(advan_mp_sample_data, sort_keys=[
    #     ('BRANDS', 'ascending'),
    #     ('DATE_RANGE_START', 'ascending')
    # ])
    # # Use indices to sort the table
    # advan_mp_sample_data = advan_mp_sample_data.take(sort_indices)

    # save_path = r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (banks, re brokerage, gas, retail).csv'
    save_path = r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (Tesla).csv'
    pv.write_csv(advan_mp_sample_data, save_path)

    # Filter data for the selected brands
    # sub_brand_names = ['Bank of America', 'Chase', 'Wells Fargo', 'REMAX', 'Century 21 Real Estate', 'Keller Williams',
    #                'Shell Oil', 'Chevron', 'Starbucks', 'Costco', 'Target', 'Walmart', 'The Home Depot']
    # sub_brand_names = ['Bank of America', 'Chase', 'Wells Fargo', 'REMAX', 'Century 21 Real Estate', 'Keller Williams']
    sub_brand_names = ['Shell Oil', 'Chevron', 'Starbucks', 'Costco', 'Target', 'Walmart', 'The Home Depot']
    sub_select_brands = unique_brands[unique_brands['BRANDS'].isin(sub_brand_names)]

    sub_select_brand_ids = set(sub_select_brands['SAFEGRAPH_BRAND_IDS'])  # Assuming select_brands is already a Pandas DataFrame or similar
    value_set=pa.array(list(sub_select_brand_ids))

    mask = pa.compute.is_in(advan_mp_sample_data['SAFEGRAPH_BRAND_IDS'], value_set=value_set)
    filtered_table = advan_mp_sample_data.filter(mask)
    # save_path = r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (banks, re brokerage).csv'
    save_path = r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (gas, retail).csv'
    pv.write_csv(filtered_table, save_path)

safegraph_poi_reserve_columns = ['PLACEKEY', 'PARENT_PLACEKEY', 'SAFEGRAPH_BRAND_IDS', 'LOCATION_NAME',
                   'BRANDS', 'STORE_ID', 'TOP_CATEGORY', 'SUB_CATEGORY', 'NAICS_CODE',
                   'LATITUDE', 'LONGITUDE', 'STREET_ADDRESS', 'CITY', 'REGION', 'POSTAL_CODE',
                   'GEOMETRY_TYPE', 'POLYGON_WKT', 'POLYGON_CLASS', 'ISO_COUNTRY_CODE', 'WKT_AREA_SQ_METERS']

advan_mp_reserve_columns = safegraph_poi_reserve_columns +\
                            ['DATE_RANGE_START', 'DATE_RANGE_END',
                             'RAW_VISIT_COUNTS', 'RAW_VISITOR_COUNTS', 'VISITS_BY_DAY', 'MEDIAN_DWELL']


read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)
advan_mp_bank_re_brokerage = pv.read_csv(r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (banks, re brokerage).csv', read_options = read_options)
advan_mp_bank_re_brokerage_df = advan_mp_bank_re_brokerage.to_pandas()
advan_mp_bank_re_brokerage_sub_df = advan_mp_bank_re_brokerage_df[advan_mp_reserve_columns]
advan_mp_bank_re_brokerage_sub_df.to_csv(r'./data/advan_mp_sub_banks_re_brokerage_all_area_all_period.csv', index=False)

advan_mp_gas_retail = pv.read_csv(r'J:/Dewey/Data/Advan_processed/advan_mp_20240419_sample (gas, retail).csv', read_options = read_options)
advan_mp_gas_retail_df = advan_mp_gas_retail.to_pandas()
advan_mp_gas_retail_sub_df = advan_mp_gas_retail_df[advan_mp_reserve_columns]
advan_mp_gas_retail_sub_df.to_csv(r'./data/advan_mp_sub_gas_retail_all_area_all_period.csv', index=False)
