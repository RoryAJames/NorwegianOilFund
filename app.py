import streamlit as st
from streamlit_option_menu import option_menu
from apps.about_page import show_about_page
from apps.historical import show_historical_page
from apps.explore import show_explore_page

st.set_page_config(layout="wide")

with st.sidebar:
    selected = option_menu("Main Menu",
                           options = ("About", "Historical Trends", "Explore"),
                           icons=("house","bar-chart-line","search"),
                           menu_icon="cast")
    
if selected == "About":
    show_about_page()

elif selected == "Historical Trends":
    show_historical_page()
    
else:
    show_explore_page()

st.sidebar.info(
        """
        This app was built by Rory James
        
        Data for this project was sourced from the [Norges Bank Investment Management website](https://www.nbim.no/en/the-fund/investments/#/)
        
        [Click Here For The Project Source Code](https://github.com/RoryAJames/NorwegianOilFund) 
        
        Feel free to connect with me:        
        [GitHub](https://github.com/RoryAJames) | [LinkedIn](https://www.linkedin.com/in/rory-james-873493111/)
        
    """
    )