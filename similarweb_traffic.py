import pandas as pd
import plotly.express as px

similarweb_traffic_sub = pd.read_csv('./data/similarweb_traffic_sub_select_DOMAIN_all_period.csv')

# Summary -------------------------
similarweb_traffic_sub.columns
len(similarweb_traffic_sub.columns)
# Unique domain list
similarweb_traffic_sub['DOMAIN'].unique()

# Period - All
print(min(similarweb_traffic_sub['YEARMONTH']))
print(max(similarweb_traffic_sub['YEARMONTH']))
# Record count by DOMAIN
similarweb_traffic_sub['DOMAIN'].value_counts()
# ----------------------------------

# Filter data only for 'DOMAIN's without 'www.'
# similarweb_traffic_sub = similarweb_traffic_sub[~similarweb_traffic_sub['DOMAIN'].str.contains('www.')]

# # Create a 'DOMAIN' column that contains the DOMAIN without the 'www.'
# similarweb_traffic_sub['DOMAIN'] = similarweb_traffic_sub['DOMAIN'].str.replace('www.', '')

# Sub dataset containing Zillow.com, Redfin.com, Realtor.com
domains_traffic = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains(
    'zillow.com|redfin.com|realtor.com', case=False, regex=True)]

# gmail vs outlook
domains_traffic = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains(
    'gmail.com|outlook.com', case=False, regex=True)]

# Korean serach potals
domains_traffic = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains(
    'naver.com|daum.net|kakao.com', case=False, regex=True)]

domains_traffic = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains(
    'openai.com', case=False, regex=True)]


# Sort by DOMAIN and YEARMONTH
domains_traffic = domains_traffic.sort_values(by=['DOMAIN', 'YEARMONTH'])
domains_traffic.reset_index(drop=True, inplace=True)

# VISITS
# Convert the DataFrame to long format
long_format_df = domains_traffic.melt(id_vars=['YEARMONTH', 'DOMAIN'], value_vars=['DESKTOP_VISITS', 'MOBILE_VISITS'],
                                  var_name='VISIT_TYPE', value_name='VISITS')

# Create a new column that combines 'DOMAIN' and 'VISIT_TYPE' for unique coloring
long_format_df['DOMAIN_VISIT_TYPE'] = long_format_df['DOMAIN'] + ' - ' + long_format_df['VISIT_TYPE']

# Now, plot with Plotly Express, using the new 'DOMAIN_VISIT_TYPE' column for color differentiation
fig = px.line(long_format_df,
              x='YEARMONTH',
              y='VISITS',
              color='DOMAIN_VISIT_TYPE',  # Differentiate by both DOMAIN and VISIT_TYPE
              markers=True,  # Adds markers to lines
              )

# Show the plot
fig.show(renderer="browser")


# Duration
# Convert the DataFrame to long format
long_format_df = domains_traffic.melt(id_vars=['YEARMONTH', 'DOMAIN'],
                                  value_vars=['DESKTOP_AVERAGE_VISIT_DURATION',
                                              'MOBILE_AVERAGE_VISIT_DURATION'],
                                  var_name='VISIT_TYPE', value_name='DURATION')

# Create a new column that combines 'DOMAIN' and 'VISIT_TYPE' for unique coloring
long_format_df['DOMAIN_VISIT_TYPE'] = long_format_df['DOMAIN'] + ' - ' + long_format_df['VISIT_TYPE']

# Now, plot with Plotly Express, using the new 'DOMAIN_VISIT_TYPE' column for color differentiation
fig = px.line(long_format_df,
              x='YEARMONTH',
              y='DURATION',
              color='DOMAIN_VISIT_TYPE',  # Differentiate by both DOMAIN and VISIT_TYPE
              markers=True,  # Adds markers to lines
              )

# Show the plot
fig.show(renderer="browser")

# For fun

# bankofamerica.com
# It's not done here but can compare with offline traffic from Advan data
# Web traffic went up during COVID-19 (BoA)
one_DOMAIN_data = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains('bankofamerica.com')]

# openai.com
one_DOMAIN_data = similarweb_traffic_sub[similarweb_traffic_sub['DOMAIN'].str.contains('openai.com')]

one_DOMAIN_data = one_DOMAIN_data.sort_values(by=['YEARMONTH'])

# Aggregate the 'DESKTOP_VISITS' and 'MOBILE_VISITS'columns by summing them by 'YEARMONTH'
one_DOMAIN_data = one_DOMAIN_data.groupby('YEARMONTH').agg({'DESKTOP_VISITS': 'sum', 'MOBILE_VISITS': 'sum'}).reset_index()

# Draw a line plot for the 'DESKTOP_VISITS' and 'MOBILE_VISITS' columns
fig = px.line(one_DOMAIN_data,
              x='YEARMONTH',
              y=['DESKTOP_VISITS', 'MOBILE_VISITS'],
              title='Desktop and Mobile Visits Over Time',
              labels={'value': 'Visits', 'YEARMONTH': 'Year-Month'},
              )
fig.show(renderer="browser")

