import pandas as pd

import os
import pyarrow.csv as pv
import pyarrow as pa
import pyarrow.compute as pc

local_dir = os.path.join(DATA_ROOT_FOLDER, 'SimilarWeb/Monthly')

process_raw_data = False

if process_raw_data:
    reserve_domains = ('zillow.com|redfin.com|realtor.com|'
                       'remax.com|century21.com|kellerwilliams.com|'
                       'bankofamerica.com|chase.com|wellsfargo.com|'
                       'shell.com|chevron.com|'
                       'starbucks.com|'
                       'costco.com|target.com|walmart.com|homedepot.com|'
                       'microsoft.com|apple.com|google.com|amazon.com|'
                       'openai.com|'
                       'naver.com|daum.net|kakao.com|'
                       'gmail.com|yahoo.com|outlook.com')

    # Add 'www.' to the reserve_domains
    reserve_www_domains = '|'.join(['www.' + domain for domain in reserve_domains.split('|')])

    # reserve_domains to a list
    reserve_domains = reserve_domains.split('|') + reserve_www_domains.split('|')

    files = os.listdir(local_dir)

    read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)

    # Read the first file
    web_traffic_sample = pv.read_csv(os.path.join(local_dir, files[0]), read_options=read_options)
    web_traffic_sample = web_traffic_sample.to_pandas()

    # 'DOMAIN' to lower case
    web_traffic_sample['DOMAIN'] = web_traffic_sample['DOMAIN'].str.lower()

    value_set=pa.array(reserve_domains)
    data_chunks = []
    for i, file in enumerate(files):
        path = os.path.join(local_dir, file)
        print(f'Processing {path}...{i+1}/{len(files)}')

        # Read the CSV file with PyArrow
        table = pv.read_csv(path, read_options = read_options)

        # 'DOMAIN' to lower case
        # Convert 'DOMAIN' column to lowercase using pyarrow.compute
        lowercased_domain = pc.utf8_lower(table['DOMAIN'])

        # Replace the original 'DOMAIN' column with the lowercased version
        table = table.set_column(table.schema.get_field_index('DOMAIN'), table.field('DOMAIN'), lowercased_domain)
        # Filter rows directly in PyArrow by creating a boolean mask and filtering the table
        # mask = pa.compute.is_in(table['SAFEGRAPH_BRAND_IDS'], value_set=pa.array(list(select_brand_ids)))
        mask = pa.compute.is_in(table['DOMAIN'], value_set=value_set)
        filtered_table = table.filter(mask)
        # filtered_table = filtered_table.to_pandas()

        data_chunks.append(filtered_table)

    # Concatenate all the data_chunks
    similar_web_sub = pa.concat_tables(data_chunks)

    save_path = os.path.join(DATA_ROOT_FOLDER, 'Similarweb_processed/similar_web_sub_select_domain_all_period.csv')
    pv.write_csv(similar_web_sub, save_path)

similarweb_traffic_sub = similar_web_sub.to_pandas()
similarweb_traffic_sub.to_csv('./data/similarweb_traffic_sub_select_domain_all_period.csv', index=False)