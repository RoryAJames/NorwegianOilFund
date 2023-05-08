import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
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

st.title("Analyzing The Norwegian Oil Fund")

st.write("""The Government Pension Fund of Norway, also known simply as the Norwegian Oil Fund, is one of the world's largest sovereign wealth funds.
             At the end of 2022, the fund had a total market value of nearly \$1.2 trillion in assets (USD), which is roughly \$233,000 per Norwegian citizen.
             The fund invests primarily in equities, fixed-income, physical real estate, and renewable energy infrastructure. The  funds investments are spread
             across most markets, countries, and currencies to achieve broad exposure to global growth while maintaining risk diversification. Assessing the funds holdings
             is a great way to get insight into how the one can construct a reliable investment portfolio. While the funds website provides users with an accessible way to
             quickly view the funds annual holdings, I found that it can lack in terms of uncovering insights into how the fund has changed over time, and how it has been
             positioning itself in recent years. As such, I wanted to build this application to let users uncover deeper insights into the funds holdings.  
             """)

st.write('')

st.write('This project consists of three parts: ')

st.markdown("- Part 1 - Exploring the historical proportions of the fund across various sectors and regions over time.")
st.markdown("- Part 2 - Analyzing the equity inflows of the fund by assessing ownership percentages.")
st.markdown("- Part 3 - Allowing a user to slice and dice the data based on their interests.")

st.write(""" A note - I decided to only focus on the funds equity and fixed income holdings in this project. The reason for this is that the vast majority of investors
         do not have the ability to buy physical real estate beyond their principal residence, and renewable energy infrastructure. 
         This project is for educational purposes only.""")

st.subheader("Part 1: Fund Proportions Over Time")

st.write("The most logical place to start the analysis is by comparing the funds equity to fixed income proportion.")

##EQUITY TO FIXED INCOME PROPORTION

equity_fi_df = run_query('SQL/eq_fi_proportions.sql')

# Plot the data using plotly
eq_fi_fig = px.line(equity_fi_df, x='year', y=['Equity Proportion', 'Fixed Income Proportion'], markers=True)

# Update the plotly figure object layout
eq_fi_fig.update_layout(title='Equity and Fixed Income Proportions Over Time', title_x = 0.3, xaxis_title='Year', yaxis_title='Proportion Of Fund (%)')
    
# Plot in streamlit
st.plotly_chart(eq_fi_fig, use_container_width=True)

st.write("When comparing the proportion of the fund by sector and type of fixed income over time...")

##SECTOR PROPORTIONS

sector_prop_df = run_query('SQL/sector_proportions.sql')

#Plot using plotly
sector_prop_fig = px.line(sector_prop_df, x="year", y="Proportion of Fund", color="Sector", title="Proportion of Fund By Sector Over Time", markers=True, facet_row="category")

sector_prop_fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

sector_prop_fig.update_layout(title_x=0.3)
    
# Adjust subplot margin and height
sector_prop_fig.update_yaxes(matches=None)
sector_prop_fig.update_layout(margin=dict(l=20, r=20, t=50, b=20), height=800)

# Plot in streamlit
st.plotly_chart(sector_prop_fig, use_container_width=True)

st.write("Insert blurb about sector proportions over time.")

##REGION PROPORTIONS

region_prop_df = run_query('SQL/region_proportions.sql')

region_prop_fig = px.line(region_prop_df, x="year", y="proportion", color="region", title="Proportion of Fund By Region Over Time", markers=True)

region_prop_fig.update_layout(title_x=0.3)

st.plotly_chart(region_prop_fig, use_container_width=True)

st.subheader("Part 2: Equity Inflows Over Time")

## AVG OWNERSHIP BY SECTOR OVER TIME

sector_ownership_df = run_query('SQL/ownership_sector.sql')

sector_ownership_fig = px.line(sector_ownership_df, x="year", y="avg_percent_ownership", color="Sector", title="Average Ownership By Sector Over Time", markers=True)

sector_ownership_fig.update_layout(title_x=0.3)

st.plotly_chart(sector_ownership_fig, use_container_width=True)


st.subheader("Part 3: Exploring Individual Countries")




