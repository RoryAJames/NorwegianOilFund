import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)

def show_historical_page():
    
    st.title('Historical Trends of The Norwegian Oil Fund')
    
    ## Equity and Fixed Income Proportions YOY
    
    eq_fi_query = """SELECT year as Year,
    ROUND((SUM(CASE WHEN category = 'Equity' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Equity Percentage",
    ROUND((SUM(CASE WHEN category = 'Fixed Income' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Fixed Income Percentage"
    FROM oil_fund
    GROUP BY year;"""
    
    eq_fi_df = run_query(eq_fi_query)
    
    # Plot the data using plotly
    eq_fi_fig = px.line(eq_fi_df, x='year', y=['Equity Percentage', 'Fixed Income Percentage'], markers=True)

    # Update the plotly figure object layout
    eq_fi_fig.update_layout(title='Equity and Fixed Income Proportions Over Time',
                      title_x = 0.3,
                      xaxis_title='Year',
                      yaxis_title='Percentage Of Fund')
    
    # Plot in streamlit
    st.plotly_chart(eq_fi_fig, use_container_width=True)
    
    ## Industry Proportions Over Time
    
    industry_prop_query = """SELECT year, industry as "Industry",
    ROUND(sum(market_value) / sum(sum(market_value)) OVER (PARTITION BY year) * 100.0,2) as "Proportion of Fund"
    FROM oil_fund
    GROUP BY year, industry;"""

    industry_prop_df = run_query(industry_prop_query)
    
    #Plot using plotly
    industry_prop_fig = px.line(industry_prop_df, x="year", y="Proportion of Fund", color="Industry", title="Proportions of Market Value by Industry over Time", markers=True)
    
    # Plot in streamlit
    st.plotly_chart(industry_prop_fig, use_container_width=True)