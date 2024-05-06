import pandas as pd
import pyarrow.csv as pv
import plotly.express as px

# Read from the saved file
read_options = pv.ReadOptions(block_size=32 * 1024 * 1024)
advan_mp_sub = pv.read_csv(r'./data/advan_mp_sub_banks_re_brokerage_all_area_all_period.csv', read_options = read_options)
advan_mp_sub = pv.read_csv(r'./data/advan_mp_sub_gas_retail_all_area_all_period.csv', read_options = read_options)
advan_mp_df = advan_mp_sub.to_pandas()

# Extract date from DATE_RANGE_START to YEAR_MONTH column
advan_mp_df['DATE_RANGE_START'] = pd.to_datetime(advan_mp_df['DATE_RANGE_START'])
# Extract YYYY-MM from SPEND_DATE_RANGE_START
advan_mp_df['YEAR_MONTH'] = advan_mp_df['DATE_RANGE_START'].dt.to_period('M').astype(str)

# Summary -------------------------
advan_mp_df.columns
len(advan_mp_df.columns)
# Unique brand list
advan_mp_df['BRANDS'].unique()

# Period - All
print(min(advan_mp_df['DATE_RANGE_START']))
print(max(advan_mp_df['DATE_RANGE_START']))
# Record count by REGION - All states
advan_mp_df['REGION'].value_counts()
# ----------------------------------

# Aggregate RAW_VISIT_COUNTS, RAW_VISITOR_COUNTS by BRANDS and YEAR_MONTH
agg_cols = ['RAW_VISIT_COUNTS', 'RAW_VISITOR_COUNTS']
advan_mp_agg = advan_mp_df.groupby(['BRANDS', 'YEAR_MONTH'])[agg_cols].sum().reset_index()

# Draw timeseries chart using plotly
# Convert YEAR_MONTH to datetime
advan_mp_agg['YEAR_MONTH'] = pd.to_datetime(advan_mp_agg['YEAR_MONTH'], format='%Y-%m')

# Plotting RAW_VISIT_COUNTS
fig_visits = px.line(advan_mp_agg, x='YEAR_MONTH', y='RAW_VISIT_COUNTS', color='BRANDS',
                     title='Time Series of Raw Visit Counts by Brand',
                     labels={'YEAR_MONTH': 'Year and Month', 'RAW_VISIT_COUNTS': 'Raw Visit Counts'},
                     markers=True)
fig_visits.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_visits.show(renderer="browser")

# Plotting RAW_VISITOR_COUNTS
fig_visitors = px.line(advan_mp_agg, x='YEAR_MONTH', y='RAW_VISITOR_COUNTS', color='BRANDS',
                       title='Time Series of Raw Visitor Counts by Brand',
                       labels={'YEAR_MONTH': 'Year and Month', 'RAW_VISITOR_COUNTS': 'Raw Visitor Counts'},
                       markers=True)
fig_visitors.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_visitors.show(renderer="browser")

# Aggregate advan_mp_df for the count of each BRAND by YEAR_MONTH
advan_mp_agg_brands = advan_mp_df.groupby(['BRANDS', 'YEAR_MONTH'])['PLACEKEY'].count().reset_index()
advan_mp_agg_brands.columns = ['BRANDS', 'YEAR_MONTH', 'PLACE_COUNTS']

# Plotting COUNTS
fig_counts = px.line(advan_mp_agg_brands, x='YEAR_MONTH', y='PLACE_COUNTS', color='BRANDS',
                     title='Time Series of Counts by Brand',
                     labels={'YEAR_MONTH': 'Year and Month', 'PLACE_COUNTS': 'Place Counts'},
                     markers=True)
fig_counts.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_counts.show(renderer="browser")

# Home Depot vs. Starbucks
# Open './data/advan_mp_sub_gas_retail_all_area_all_period.csv' first.
# Filter for Home Depot and Starbucks
sub_advan_mp_agg = advan_mp_agg[advan_mp_agg['BRANDS'].isin(['The Home Depot', 'Starbucks'])]

# plot sub_advan_mp_agg
fig_sub_visitors = px.line(sub_advan_mp_agg, x='YEAR_MONTH', y='RAW_VISIT_COUNTS', color='BRANDS',
                         title='Time Series of Counts by Brand',
                         labels={'YEAR_MONTH': 'Year and Month', 'RAW_VISIT_COUNTS': 'Raw Visit Counts'},
                         markers=True)
fig_sub_visitors.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_sub_visitors.show(renderer="browser")

# Scale to the first month for each brand
sub_advan_mp_agg.loc[:, 'VISIT_COUNTS'] = sub_advan_mp_agg['RAW_VISIT_COUNTS']
sub_advan_mp_agg.loc[:, 'VISIT_COUNTS'] = sub_advan_mp_agg.groupby('BRANDS')['VISIT_COUNTS'].transform(lambda x: x / x.iloc[0])

# Plot scaled data
fig_sub_visitors_scaled = px.line(sub_advan_mp_agg, x='YEAR_MONTH', y='VISIT_COUNTS', color='BRANDS',
                         title='Time Series of Counts by Brand',
                         labels={'YEAR_MONTH': 'Year and Month', 'VISIT_COUNTS': 'Scaled Visit Counts'},
                         markers=True)
fig_sub_visitors_scaled.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_sub_visitors_scaled.show(renderer="browser")

# Bank visits by state by month
# Open './data/advan_mp_sub_banks_re_brokerage_all_area_all_period.csv' first.
# Agrregate advan_mp_df by state, brand, and year_month
advan_mp_agg_state = advan_mp_df.groupby(['REGION', 'BRANDS', 'YEAR_MONTH'])['RAW_VISIT_COUNTS'].sum().reset_index()

# Filter for Bank of America
advan_mp_agg_boa = advan_mp_agg_state[advan_mp_agg_state['BRANDS'] == 'Bank of America']
# Filter REGION for CA, FL, TX, NY, GA, NC, SC, OH
advan_mp_agg_boa = advan_mp_agg_boa[advan_mp_agg_boa['REGION'].isin(['CA', 'FL', 'TX', 'NY', 'GA', 'NC', 'SC', 'OH'])]
# Plot Bank of America visits by state by month
fig_abrand = px.line(advan_mp_agg_boa, x='YEAR_MONTH', y='RAW_VISIT_COUNTS', color='REGION',
                  title='Time Series of Bank of America Visits by State',
                  labels={'YEAR_MONTH': 'Year and Month', 'RAW_VISIT_COUNTS': 'Raw Visit Counts'},
                  markers=True)
fig_abrand.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_abrand.show(renderer="browser")

# Scale to the first month for each state
advan_mp_agg_boa['VISIT_COUNTS'] = advan_mp_agg_boa['RAW_VISIT_COUNTS']
advan_mp_agg_boa['VISIT_COUNTS'] = advan_mp_agg_boa.groupby('REGION')['VISIT_COUNTS'].transform(lambda x: x / x.iloc[0])
# Plot scaled data
fig_abrand_scaled = px.line(advan_mp_agg_boa, x='YEAR_MONTH', y='VISIT_COUNTS', color='REGION',
                         title='Time Series of Bank of America Visits by State',
                         labels={'YEAR_MONTH': 'Year and Month', 'VISIT_COUNTS': 'Scaled Visit Counts'},
                         markers=True)
fig_abrand_scaled.update_xaxes(dtick="M1", tickformat="%b\n%Y")
fig_abrand_scaled.show(renderer="browser")



