import pandas as pd
from extract import extract_data

def transform_data():

    df = extract_data()
    
    print(df.columns)

transform_data()