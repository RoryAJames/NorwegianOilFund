import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np
import psycopg2

st.set_page_config(page_title="Analyzing The Norwegian Oil Fund", layout="wide")

st.sidebar.info(
    """
    This app was built by Rory James
    
    Data for this project was sourced from the [Norges Bank Investment Management website](https://www.nbim.no/en/the-fund/investments/#/)
    
    [Click Here For The Project Source Code](https://github.com/RoryAJames/NorwegianOilFund)
    
    Feel free to connect with me:
    [GitHub](https://github.com/RoryAJames) | [LinkedIn](https://www.linkedin.com/in/rory-james-873493111/)
    
    """
    )

# Initialize connection to Postgres DB. Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

conn = init_connection()
cur = conn.cursor()


# Function that executes SQL queries and returns the results as a pandas dataframe. Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query_path):
    with open(query_path, 'r') as file:
        query = file.read()
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)

# Function that allows for multiselect in countries section    
@st.cache_data(ttl=600)
def run_query_dynamic_country(query_path, num_years, countries):
    with open(query_path, 'r') as file:
        query = file.read()
    placeholders = ','.join(['%s'] * len(countries))
    formatted_query = query.format(placeholders)
    with conn.cursor() as cur:
        cur.execute(formatted_query, (num_years, *countries))
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)

st.title("Analyzing The Norwegian Oil Fund")

st.write("""The Government Pension Fund of Norway, also known simply as the Norwegian Oil Fund, is one of the world's largest sovereign wealth funds.
             At the end of 2022, the fund had a total market value of nearly \$1.2 trillion in assets (USD), which is roughly \$233,000 per Norwegian citizen.
             The fund invests primarily in equities, fixed-income, physical real estate, and renewable energy infrastructure. The  funds investments are spread
             across most markets, countries, and currencies to achieve broad exposure to global growth while maintaining risk diversification. Assessing the funds holdings
             is a great way to get insight into how the one can construct a reliable investment portfolio. While the funds website provides users with an accessible way to
             quickly view the funds annual holdings, I found that it can lack in terms of uncovering insights into how the fund has changed over time, and how it has been
             positioning itself in recent years. As such, I wanted to build this application to let users uncover deeper insights into the funds holdings.  
             """)

st.write('This project consists of three parts: ')

st.markdown("- Part 1 - Exploring the historical proportions of the fund over time.")
st.markdown("- Part 2 - Analyzing the equity inflows of the fund by assessing ownership percentages.")
st.markdown("- Part 3 - Allowing a user to slice and dice the data based on their interests.")

st.write(""" A note - I decided to only focus on the funds equity and fixed income holdings for this project. The reason for this is that the vast majority of investors
         do not have the ability to buy physical real estate beyond their principal residence, and renewable energy infrastructure. This project is for educational purposes.""")

st.subheader("Part 1: Fund Proportions Over Time")

st.write("""The most logical place to start the analysis is comparing the funds equity to fixed income proportions. Prior to the 2008 financial crisis, a majority of the funds
         assets were in infixed income investments. During the financial crisis, central banks around the world were forced to implement unconventional monetary policy measures such as
         quantitative easing (QE), which involved buying large amounts of government bonds and other securities to inject liquidity into the financial system. As a result of QE,
         bond yields fell, making them less attractive to investors. This clearly played out for the oil fund as it shifted to a typical 60/40 equity to fixed income
         portfolio in 2009, which was maintained for half a decade. In 2017, the Norwegian government announced that it would increase the equity allocation from 60\% to 70\% 
         of the fund's total assets over a period of time. The goal was to improve the fund's long-term return prospects by increasing exposure to equity markets, which
         historically provided higher returns compared to fixed income investments. The equity proportion of the fund peaked in 2020 at nearly 74\% of total assets.
         Since the covid pandemic the equity proportion has gradually subsided. While this could be attributed to equity markets coming under recent pressure, I will note that bond
         yields have also risen recently and have become more attractive on a risk adjusted basis for pension funds.""")

##EQUITY TO FIXED INCOME PROPORTION

equity_fi_df = run_query('SQL/static/eq_fi_proportions.sql')

# Plot the data using plotly
eq_fi_fig = px.line(equity_fi_df, x='year', y=['Equity Proportion', 'Fixed Income Proportion'], markers=True)
eq_fi_fig.update_traces(mode="markers+lines", hovertemplate=None)

# Update the plotly figure object layout
eq_fi_fig.update_layout(title='Equity and Fixed Income Proportions Over Time', title_x = 0.4, xaxis_title='Year', yaxis_title='Proportion Of Fund (%)',
                        hovermode="x unified", legend_title = "")
    
# Plot in streamlit
st.plotly_chart(eq_fi_fig, use_container_width=True)

##SECTOR PROPORTIONS

st.write("When comparing the various sectors and type of fixed income over time...")

sector_prop_df = run_query('SQL/static/sector/sector_proportions.sql')

# Create a dictionary of colors for each sector
equity_colors = {'Basic Materials': '#FFA07A',
                 'Consumer Discretionary': '#20B2AA',
                 'Consumer Staples': '#87CEFA',
                 'Energy': '#B0E0E6',
                 'Financials': '#7B68EE',
                 'Health Care': '#FF7F50',
                 'Industrials': '#6495ED',
                 'Real Estate': '#9ACD32',
                 'Technology': '#F08080',
                 'Telecommunications': '#DDA0DD',
                 'Utilities': '#00FFFF'}

fixed_income_colors = {'Corporate Bonds': '#FFD700',
                       'Government Bonds': '#CD5C5C',
                       'Securitized Bonds': '#ADFF2F',
                       'Treasuries': '#2F4F4F'}

# Initialize figure with subplots
sector_prop_fig = make_subplots(rows=2, cols=1, subplot_titles=("Equity Investments", "Fixed Income Investments"))

# Create line plots for each sector within each category
for i, category in enumerate(sector_prop_df['category'].unique()):
    sector_df = sector_prop_df[sector_prop_df['category'] == category]
    for j, sector in enumerate(sector_df['Sector'].unique()):
        sector_data = sector_df[sector_df['Sector'] == sector]
        if category == 'Equity':
            sector_prop_fig.add_trace(go.Scatter(x=sector_data['year'], y=sector_data['Proportion of Fund'], 
                                     mode='lines+markers', name=sector, 
                                     line=dict(color=equity_colors[sector]), showlegend=i==0), row=i+1, col=1)
        else:
            sector_prop_fig.add_trace(go.Scatter(x=sector_data['year'], y=sector_data['Proportion of Fund'], 
                                     mode='lines+markers', name=sector, 
                                     line=dict(color=fixed_income_colors[sector]), showlegend=True), row=i+1, col=1)

# Update xaxis and yaxis properties
sector_prop_fig.update_xaxes(title_text='Year', row=2, col=1)
sector_prop_fig.update_yaxes(title_text='Proportion Of Fund (%)', row=1, col=1)
sector_prop_fig.update_yaxes(title_text='Proportion Of Fund (%)', row=2, col=1)

# Update subplot titles
sector_prop_fig.update_layout(title='Sector and Fixed Income Proportions Over Time', height=800, margin=dict(t=120), title_x = 0.35)

# Set subplot titles
sector_prop_fig.update_annotations(
    {'text': 'Equity Investments', 'font': {'size': 24}, 'x': 0.5, 'y': 1.05, 'showarrow': False},
    {'text': 'Fixed Income Investments', 'font': {'size': 24}, 'x': 0.5, 'y': 1.05, 'showarrow': False}
)

# Show legend for each subplot
sector_prop_fig.update_layout(showlegend=True)

# Display plot in Streamlit
st.plotly_chart(sector_prop_fig, use_container_width=True)

st.write("Insert blurb about sector proportions over time.")

##REGION PROPORTIONS

region_prop_df = run_query('SQL/static/region/region_proportions.sql')

region_prop_fig = px.line(region_prop_df, x="year", y="proportion", color="Region", title="Proportion of Fund By Region Over Time", markers=True)

region_prop_fig.update_layout(title_x=0.3)
region_prop_fig.update_xaxes(title_text='Year')
region_prop_fig.update_yaxes(title_text='Proportion Of Fund (%)')

st.plotly_chart(region_prop_fig, use_container_width=True)

st.subheader("Part 2: Equity Inflows Over Time")

## AVG OWNERSHIP BY SECTOR OVER TIME

sector_ownership_df = run_query('SQL/static/sector/sector_ownership.sql')

sector_ownership_fig = px.line(sector_ownership_df, x="year", y="avg_percent_ownership", color="Sector", title="Average Ownership By Sector Over Time", markers=True)

sector_ownership_fig.update_layout(title_x=0.3)
sector_ownership_fig.update_xaxes(title_text='Year')
sector_ownership_fig.update_yaxes(title_text='Average Ownership (%)')

st.plotly_chart(sector_ownership_fig, use_container_width=True)

## Cumulative Change In Percent Ownership By Sector - Last 10 Years

cum_owner_change_sector_10_df = run_query('SQL/static/sector/ownership_change_sector_ten_years.sql')

cum_owner_change_sector_10_fig = px.bar(cum_owner_change_sector_10_df, x = 'Sector', y='cumulative_bp_change_of_ownership', title= "Cumulative Change In Average Ownership By Sector - Last 10 Years",
                                              text_auto= True)

cum_owner_change_sector_10_fig.update_layout(title_x=0.3)
cum_owner_change_sector_10_fig.update_yaxes(title_text='Cumulative Change In Ownership (Basis Points)')

st.plotly_chart(cum_owner_change_sector_10_fig, use_container_width=True)

## Cumulative Change In Percent Ownership and Market Value By Sector - Last 10 Years

mrkt_value_ownership_change_sector_ten_years_df = run_query('SQL/static/sector/mrkt_value_ownership_change_sector_ten_years.sql')

mrkt_value_ownership_change_sector_ten_years_fig = px.scatter(mrkt_value_ownership_change_sector_ten_years_df, 
                                                              x= 'Cumulative Average Ownership Change', y= 'Cumulative Market Value Percent Change',
                                                              color='Sector',
                                                              title="Cumulative Change In Ownership and Market Value By Sector - Last 10 Years")

mrkt_value_ownership_change_sector_ten_years_fig.update_traces(marker_size=10)
mrkt_value_ownership_change_sector_ten_years_fig.update_layout(title_x=0.3, showlegend=False)
mrkt_value_ownership_change_sector_ten_years_fig.update_xaxes(title_text='Cumulative Change In Average Ownership (Basis Points)')
mrkt_value_ownership_change_sector_ten_years_fig.update_yaxes(title_text='Cumulative Change In Market Value (%)')

st.plotly_chart(mrkt_value_ownership_change_sector_ten_years_fig, use_container_width=True)

## AVG OWNERSHIP BY REGION OVER TIME

st.write("""While North America is the largest regional proportion of the fund, when assessing the average ownership it doesn't appear that the fund has been buying into the
         North American markets relative to other regions.
         """)

region_ownership_df = run_query('SQL/static/region/region_ownership.sql')

region_ownership_fig = px.line(region_ownership_df, x="year", y="avg_percent_ownership", color="Region", title="Average Ownership By Region Over Time", markers=True)

region_ownership_fig.update_layout(title_x=0.3)
region_ownership_fig.update_xaxes(title_text='Year')
region_ownership_fig.update_yaxes(title_text='Average Ownership (%)')

st.plotly_chart(region_ownership_fig, use_container_width=True)

## AVG OWNERSHIP BY MSCI MARKET TYPE OVER TIME

MSCI_ownership_df = run_query('SQL/static/region/MSCI_ownership.sql')

MSCI_ownership_fig = px.line(MSCI_ownership_df, x="year", y="avg_percent_ownership", color="msci_market", title="Average Ownership By MSCI Market Type Over Time", markers=True)

MSCI_ownership_fig.update_layout(title_x=0.3, hovermode="x unified", legend_title = "")
MSCI_ownership_fig.update_xaxes(title_text='Year')
MSCI_ownership_fig.update_yaxes(title_text='Average Ownership (%)')
MSCI_ownership_fig.update_traces(mode="markers+lines", hovertemplate=None)

with st.expander("Click Here To See Data Based On MSCI Market Classification"):
    st.plotly_chart(MSCI_ownership_fig, use_container_width=True)


st.subheader("Part 3: Exploring Individual Countries")

#List of countries in the database that is passed to multiselect 
countries_df = run_query('SQL/static/distinct_countries.sql')

row1_col1, row1_col2 = st.columns([1,1])

with row1_col1:
    country_selection = st.multiselect('Select Countries Of Interest: ',countries_df, default=['Canada', 'United States','Mexico'])
    
with row1_col2:
    year_selection = st.number_input('Select Number of Years', min_value= 0, max_value= 20, value= 1)
    
#Preset list of countries based on international forums.

g7_countries = ['Canada','France','Germany','Italy','Japan','United States','United Kingdom']

g20_countries = ['Canada','France','Germany','Italy','Japan','United States','United Kingdom', 'Argentina','Australia','Brazil','China','India','Indonesia','South Korea',
                 'Mexico','Russia','Saudi Arabia','South Africa','Turkey']

nato_countries = ['Canada','France','Germany','Italy','Belgium','United States','United Kingdom','Bulgaria','Croatia','Czech Republic','Denmark','Estonia','Finland',
                  'Greece','Hungary','Iceland','Latvia','Lithuania','Luxembourg','Netherlands','Poland','Portugal','Romania','Slovakia','Slovenia','Spain','Turkey']

apec_countries = ['Canada','United States','Australia','Chile','China','Hong Kong','Indonesia','Japan','South Korea','Malaysia','Mexico','New Zealand','Papua New Guinea',
                  'Peru','Philippines','Russia','Sinqapore','Taiwan','Thailand','Vietnam']

msci_developed_markets = countries = ['Australia','Austria','Belgium','Canada','Denmark','Finland','France','Germany','Hong Kong','Ireland','Israel','Italy','Japan',
                                      'Netherlands','New Zealand','Norway','Portugal','Singapore','South Korea','Spain','Sweden','Switzerland','United Kingdom',
                                      'United States']


msci_emerging_markets = ['Brazil','Chile','China','Colombia','Czech Republic','Egypt','Greece','Hungary','Indonesia','India','South Korea','Mexico','Malaysia',
                        'Peru','Philippines','Poland','Qatar','Saudi Arabia','South Africa','Thailand','Turkey','Taiwan','United Arab Emirates']

msci_latin_america = ['Brazil', 'Mexico', 'Chile', 'Colombia', 'Peru', 'Argentina']

country_custom_selection = st.selectbox('Custom Selection',['None','G7 Countries', 'G20 Countries', 'NATO Countries', 'APEC Countries','MSCI Developed Countries',
                                                            'MSCI Emerging Markets','MSCI Latin America Countries'])

#Change country selection based on user input. If a user has already made a selection the custom selection list will get added to the selection.

if country_custom_selection == 'None':
    country_selection = country_selection

elif country_custom_selection == 'G7 Countries':
    country_selection = country_selection + g7_countries

elif country_custom_selection == 'G20 Countries':
    country_selection = country_selection + g20_countries
    
elif country_custom_selection == 'NATO Countries':
    country_selection = country_selection + nato_countries

elif country_custom_selection == 'APEC Countries':
    country_selection = country_selection + apec_countries

elif country_custom_selection == 'MSCI Developed Countries':
    country_selection = country_selection + msci_developed_markets
    
elif country_custom_selection == 'MSCI Emerging Markets':
    country_selection = country_selection + msci_emerging_markets
    
elif country_custom_selection == 'MSCI Latin America Countries':
    country_selection = country_selection + msci_latin_america
    
## AVG OWNERSHIP MULTISELECT

avg_ownership_country_dynamic_df = run_query_dynamic_country('SQL/dynamic/avg_ownership_country_multiselect.sql',year_selection,country_selection)

avg_ownership_country_dynamic_fig = px.line(avg_ownership_country_dynamic_df, x="year", y="avg_percent_ownership", color="Country", title=f"Average Ownership By Country Over {year_selection} Years", markers=True)

avg_ownership_country_dynamic_fig.update_layout(title_x = 0.3, xaxis = dict(tickmode='array',tickvals = avg_ownership_country_dynamic_df['year'])) #Prevents plotly from adjusting the xaxis values.
avg_ownership_country_dynamic_fig.update_xaxes(title_text='Year')
avg_ownership_country_dynamic_fig.update_yaxes(title_text='Average Ownership (%)')

st.plotly_chart(avg_ownership_country_dynamic_fig, use_container_width=True)

## AVG OWNERSHIP CHANGE MULTISELECT
        
ownership_change_country_dynamic_df = run_query_dynamic_country('SQL/dynamic/ownership_change_country_multiselect.sql',year_selection,country_selection)

fig_ownership_change_country_fig = px.bar(ownership_change_country_dynamic_df, x = 'Country', y='cumulative_bp_change_of_ownership', title = f"Cumulative Ownership Change By Country Over {year_selection} Years",
                                              text_auto= True)

fig_ownership_change_country_fig.update_layout(title_x = 0.3)

fig_ownership_change_country_fig.update_yaxes(title_text='Cumulative Change In Ownership (Basis Points)')

st.plotly_chart(fig_ownership_change_country_fig, use_container_width=True)

## TOP 10 COMPANIES BY CUMULATIVE OWNERSHIP CHANGE MULTISELECT

top10_ownership_change_country_dynamic_df = run_query_dynamic_country('SQL/dynamic/top10_ownership_change_company_multiselect.sql',year_selection,country_selection)

top10_ownership_change_country_fig = px.bar(top10_ownership_change_country_dynamic_df, x = 'Company', y='cumulative_bp_change_of_ownership', title = f"Top 10 Companies By Cumulative Ownership Change - Last {year_selection} Years",
                                              text_auto= True)

top10_ownership_change_country_fig.update_layout(title_x = 0.3)

top10_ownership_change_country_fig.update_yaxes(title_text='Cumulative Change In Ownership (Basis Points)')

st.plotly_chart(top10_ownership_change_country_fig, use_container_width=True)