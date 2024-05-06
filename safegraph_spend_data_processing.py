import pandas as pd
import os
import pyarrow.csv as pv

# --------------------------------------------------------------
# For select brands
# Full period data
# --------------------------------------------------------------
local_dir = r'J:/Dewey/Data/SafeGraph/Spend'

files = os.listdir(local_dir)
# read the first file
spend_sample = pd.read_csv(os.path.join(local_dir, files[0]))

# Unique brand names
# unique_brands = pd.DataFrame(spend_sample['BRANDS'].unique())
unique_brands = spend_sample[['BRANDS', 'SAFEGRAPH_BRAND_IDS']].drop_duplicates()
unique_brands.dropna(inplace = True)
unique_brands.sort_values(by = ['BRANDS'], inplace = True)
unique_brands[unique_brands['BRANDS'].str.contains('shell|chevron|starbucks|costco|target|walmart|home depot', case=False)]
brand_names = ['Shell Oil', 'Chevron', 'Starbucks', 'Costco', 'Target', 'Walmart', 'The Home Depot']
select_brands = unique_brands[unique_brands['BRANDS'].isin(brand_names)]
select_brands

# Filter for the selected brands for all the files
# List to store each chunk of data
data_chunks = []
for file in files:
    path = os.path.join(local_dir, file)
    print(f'Processing {path}...')
    # Read the CSV file, filtering rows simultaneously
    chunk = pd.read_csv(path)
    filtered_chunk = chunk[chunk['SAFEGRAPH_BRAND_IDS'].isin(select_brands['SAFEGRAPH_BRAND_IDS'])]
    data_chunks.append(filtered_chunk)

sg_spend_data = pd.concat(data_chunks, ignore_index=True)

save_path = r'J:/Dewey/Data/SafeGraph/Spend_processed/sg_spend_sample.csv'
sg_spend_data.to_csv(save_path, index=False)

# Reading --------------------------------------------------------------
# sg_spend_data = pd.read_csv(save_path)
read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)
sg_spend_data = pv.read_csv(save_path, read_options = read_options)
sg_spend_data = sg_spend_data.to_pandas()

safegraph_spend_reserve_columns = \
    ['PLACEKEY', 'SAFEGRAPH_BRAND_IDS', 'BRANDS', 'SPEND_DATE_RANGE_START',
     'SPEND_DATE_RANGE_END', 'RAW_TOTAL_SPEND', 'RAW_NUM_TRANSACTIONS',
     'RAW_NUM_CUSTOMERS', 'MEDIAN_SPEND_PER_TRANSACTION',
     'MEDIAN_SPEND_PER_CUSTOMER',
     'SPEND_BY_DAY', 'ONLINE_TRANSACTIONS', 'ONLINE_SPEND',
     'TRANSACTION_INTERMEDIARY', 'SPEND_BY_TRANSACTION_INTERMEDIARY']

safegraph_spend_sub = sg_spend_data[safegraph_spend_reserve_columns]
safegraph_spend_sub.to_csv(r'./data/safegraph_spend_sub_for_select_brands_all_periods.csv', index=False)