
import pandas as pd
import plotly.express as px
import pyarrow.csv as pv
import time
# --------------------------------------------------------------
time1 = time.time()
safegraph_spend_sub = pd.read_csv(r'./data/safegraph_spend_sub_for_select_brands_all_periods.csv')
print(f'Pandas read_csv: {time.time() - time1:.2f} seconds')

# vs
time1 = time.time()
read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)
safegraph_spend_sub = pv.read_csv(r'./data/safegraph_spend_sub_for_select_brands_all_periods.csv', read_options = read_options)
safegraph_spend_sub = safegraph_spend_sub.to_pandas()
print(f'PyArrow read_csv: {time.time() - time1:.2f} seconds')

# Extract date from SPEND_DATE_RANGE_START to SPEND_MONTH column
safegraph_spend_sub['SPEND_DATE_RANGE_START'] = pd.to_datetime(safegraph_spend_sub['SPEND_DATE_RANGE_START'])
# Extract YYYY-MM from SPEND_DATE_RANGE_START
safegraph_spend_sub['SPEND_MONTH'] = safegraph_spend_sub['SPEND_DATE_RANGE_START'].dt.to_period('M').astype(str)
safegraph_spend_sub.columns

# Aggregate RAW_TOTAL_SPEND, RAW_NUM_TRANSACTIONS, RAW_NUM_CUSTOMERS, ONLINE_TRANSACTIONS, and ONLINE_SPEND by BRANDS and SPEND_MONTH
agg_cols = ['RAW_TOTAL_SPEND', 'RAW_NUM_TRANSACTIONS', 'RAW_NUM_CUSTOMERS', 'ONLINE_TRANSACTIONS', 'ONLINE_SPEND']
sg_pend_agg = safegraph_spend_sub.groupby(['BRANDS', 'SPEND_MONTH'])[agg_cols].sum().reset_index()

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
brand_data.loc[:, 'RAW_TOTAL_SPEND'] = brand_data.groupby('BRANDS')['RAW_TOTAL_SPEND'].transform(lambda x: x / x.iloc[0])

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

