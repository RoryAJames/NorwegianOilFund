import streamlit as st
from streamlit_option_menu import option_menu
from apps.about_page import show_about_page
from apps.historical import show_historical_page
from apps.explore import show_explore_page

st.set_page_config(page_title="Analyzing The Norwegian Oil Fund", layout="wide")

# A dictionary of apps in the format of {"App title": "App icon"}

apps = [
    {"func": show_about_page, "title": "About", "icon": "house"},
    {"func": show_historical_page, "title": "Historical Trends", "icon": "bar-chart-line"},
    {"func": show_explore_page, "title": "Explore", "icon": "search"}
]

titles = [app["title"] for app in apps]
titles_lower = [title.lower() for title in titles]
icons = [app["icon"] for app in apps]

params = st.experimental_get_query_params()

if "page" in params:
    default_index = int(titles_lower.index(params["page"][0].lower()))
else:
    default_index = 0

with st.sidebar:
    selected = option_menu(
        "Main Menu",
        options=titles,
        icons=icons,
        menu_icon="cast",
        default_index=default_index,
    )
    
    st.sidebar.info(
            """
            This app was built by Rory James
            
            Data for this project was sourced from the [Norges Bank Investment Management website](https://www.nbim.no/en/the-fund/investments/#/)
            
            [Click Here For The Project Source Code](https://github.com/RoryAJames/NorwegianOilFund) 
            
            Feel free to connect with me:        
            [GitHub](https://github.com/RoryAJames) | [LinkedIn](https://www.linkedin.com/in/rory-james-873493111/)
            
        """
        )

for app in apps:
    if app["title"] == selected:
        app["func"]()
        break