import pandas as pd
import os
import plotly.express as px
import pyarrow.csv as pv
import pyarrow as pa

process_raw_data = False

if process_raw_data:
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

    sg_pend_data = pd.concat(data_chunks, ignore_index=True)

    save_path = r'J:/Dewey/Data/SafeGraph/Spend_processed/sg_spend_sample.csv'
    sg_pend_data.to_csv(save_path, index=False)


# --------------------------------------------------------------
save_path = r'J:/Dewey/Data/SafeGraph/Spend_processed/sg_spend_sample.csv'
# sg_pend_data = pd.read_csv(save_path)
read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)
sg_pend_data = pv.read_csv(save_path, read_options = read_options)
sg_pend_data = sg_pend_data.to_pandas()

# Extract date from SPEND_DATE_RANGE_START to SPEND_MONTH column
sg_pend_data['SPEND_DATE_RANGE_START'] = pd.to_datetime(sg_pend_data['SPEND_DATE_RANGE_START'])
# Extract YYYY-MM from SPEND_DATE_RANGE_START
sg_pend_data['SPEND_MONTH'] = sg_pend_data['SPEND_DATE_RANGE_START'].dt.to_period('M').astype(str)
sg_pend_data.columns

# Aggregate RAW_TOTAL_SPEND, RAW_NUM_TRANSACTIONS, RAW_NUM_CUSTOMERS, ONLINE_TRANSACTIONS, and ONLINE_SPEND by BRANDS and SPEND_MONTH
agg_cols = ['RAW_TOTAL_SPEND', 'RAW_NUM_TRANSACTIONS', 'RAW_NUM_CUSTOMERS', 'ONLINE_TRANSACTIONS', 'ONLINE_SPEND']
sg_pend_agg = sg_pend_data.groupby(['BRANDS', 'SPEND_MONTH'])[agg_cols].sum().reset_index()

# Draw timeseries chart using plotly
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Starbucks']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'The Home Depot']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Target']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Walmart']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Costco']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Shell Oil']  # Replace 'SelectedBrand' with a real brand name from your data
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'] == 'Chevron']  # Replace 'SelectedBrand' with a real brand name from your data

# Create a line chart for a brand ------------------------
fig = px.line(brand_data, x='SPEND_MONTH', y=agg_cols, markers=True,
              title='Monthly Aggregated Metrics by Brand',
              labels={'value': 'Aggregated Values', 'variable': 'Metrics'})

# Update the layout to make it more informative
fig.update_layout(
    xaxis_title='Month',
    yaxis_title='Value',
    legend_title='Metric',
    hovermode='x unified'
)

fig.show(renderer="browser")

# Chart for RAW_TOTAL_SPEND and ONLINE_SPEND for a brand -----------
# Create a line chart
sub_cols = ['RAW_TOTAL_SPEND', 'ONLINE_SPEND']
fig = px.line(brand_data, x='SPEND_MONTH', y=sub_cols, markers=True,
              title=f'Monthly Aggregated Metrics for {brand_data["BRANDS"].iloc[0]}',
              labels={'value': 'Aggregated Values', 'variable': 'Metrics'})

# Update the layout to make it more informative
fig.update_layout(
    xaxis_title='Month',
    yaxis_title='Value',
    legend_title='Metric',
    hovermode='x unified'
)

fig.show(renderer="browser")


# Chart for RAW_TOTAL_SPEND for Starbucks and The Home Depot ----------------------
# Filter for Starbucks and The Home Depot
brand_data = sg_pend_agg[sg_pend_agg['BRANDS'].isin(['Starbucks', 'The Home Depot'])]

# Make RAW_TOTAL_SPEND relative to the first month
brand_data['RAW_TOTAL_SPEND'] = brand_data.groupby('BRANDS')['RAW_TOTAL_SPEND'].transform(lambda x: x / x.iloc[0])

# Create a line chart
fig = px.line(brand_data, x='SPEND_MONTH', y='RAW_TOTAL_SPEND', color='BRANDS', markers=True,
              title='Monthly Aggregated Total Spend for Starbucks and The Home Depot',
              labels={'RAW_TOTAL_SPEND': 'Total Spend', 'SPEND_MONTH': 'Month'})

# Update the layout to make it more informative
fig.update_layout(
    xaxis_title='Month',
    yaxis_title='Total Spend',
    legend_title='Brand',
    hovermode='x unified'
)

fig.show(renderer="browser")

