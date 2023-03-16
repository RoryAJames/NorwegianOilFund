import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# Initialize connection.
# Uses st.cache_resource to only run once.
#@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
#@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)

def show_historical_page():
    
    st.title('Historical Trends of The Norwegian Oil Fund')
    
    query = """SELECT year as Year,
    ROUND((SUM(CASE WHEN category = 'Equity' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Equity Percentage",
    ROUND((SUM(CASE WHEN category = 'Fixed Income' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Fixed Income Percentage"
    FROM oil_fund
    GROUP BY year;"""
    
    df = run_query(query)
    
    # create a plotly figure object
    fig = px.line(df, x='year', y=['Equity Percentage', 'Fixed Income Percentage'], markers=True)

    # update the plotly figure object layout
    fig.update_layout(title='Equity and Fixed Income Proportions Over Time',
                      title_x = 0.5,
                      xaxis_title='Year',
                      yaxis_title='Percentage Of Fund')
    
    # Plot
    st.plotly_chart(fig, use_container_width=True)