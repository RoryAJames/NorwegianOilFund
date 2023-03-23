import os
import glob
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine
from pathlib import Path
from prefect import task, flow

'''A function that extracts all of the csv files from the data folder, creates a unique identifier for each CSV file, extracts the investment category and year, 
 and then combines the files together into one dataframe'''  

#@task
def extract_data():
    
    os.chdir('C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/')

    extension = 'csv'

    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

    dfs_to_concat = []

    for f in all_filenames:
        df = pd.read_csv(f, thousands=r',') #Removes thousands separator when reading in CSV file. This ensures the values are integers.
        
        df['file_identifier'] = [f] * len(df.index) #Creates a file identifier for each csv file
        
        #Create investment category column by extracting the first two characters from file identifier
        df['category'] = df['file_identifier'].apply(lambda x: 'Equity' if x[:2] == 'EQ' else 'Fixed Income')
        
        #Create year column from file identifier
        df['year'] = df['file_identifier'].str[3:7].astype(int)
        
        dfs_to_concat.append(df)
    
    # Concat dataframes together to create a combined dataframe
    df = pd.concat(dfs_to_concat)
    
    return df

#A function that performs all of the data cleaning tasks before loading into the database.

#@task
def transform_data(data):

    #Drop unwanted columns
    
    cols_to_drop = ['Market Value(NOK)','Voting','Ownership','Incorporation Country','file_identifier']

    data.drop(columns=cols_to_drop, inplace=True)
    
    #Rename market value column to 'Value'

    new_col = {'Market Value(USD)':'market_value'}
    data.rename(columns= new_col, inplace = True)
    
    #Make all of the columns lowercase so that you can properly query the database in postgres
    
    data.columns = data.columns.str.lower()
    
    #Assign countries to their appropriate regions 

    data['region'] = data['region'].replace({'Australia':'Oceania',
                                         'New Zealand':'Oceania',
                                         'Japan':'Asia'})
    
    #Fix spelling of various countries

    data['country'] = data['country'].replace({'Faeroe Islands':'Faroe Islands',
                                           'Guernsey C. I.':'Guernsey',
                                           'Gurensey':'Guernsey',
                                           'Jersey C.I.':'Jersey',
                                           'Lichtenstein':'Liechtenstein',
                                           'Tanzania *, United Republic of':'Tanzania'})
    
    # Fix Guernsey in industry

    data['industry'] = data['industry'].replace({'Guernsey':'Financials'})

    # Fix unknown in industry, assign the values based on company name

    data['industry'] = np.where((data['industry'] == 'Unknown') & (data['name'] == 'Craft Oil Ltd'), 'Energy',
                                np.where((data['industry'] == 'Unknown') & (data['name'] == 'Kontron S&T AG'), 'Technology',
                                         data['industry']))
    
    #Assign consumer services to discretionary and consumer goods to consumer staples
    
    data['industry'] = data['industry'].replace({'Consumer Services':'Consumer Discretionary',
                                             'Consumer Goods':'Consumer Staples'})
    
    #Assign oil and gas to energy
    
    data['industry'] = data['industry'].replace({'Oil & Gas':'Energy'})
    
    #Create a list of all the unique real estate companies.
    
    real_estate_companies = data['name'].loc[data['industry']== 'Real Estate'].unique().tolist()
    
    # Up until 2020 real estate companies were categorized as financials. If a company name is in the real estate list change the industry to real estate.
    
    data['industry'] = np.where((data['name'].isin(real_estate_companies)),'Real Estate',data['industry'])
    
    #If a company is in real estate and fixed income change the industry to corporate bonds
    
    data['industry'] = np.where((data['industry'] == 'Real Estate') & (data['category'] == 'Fixed Income'), 'Corporate Bonds',data['industry'])
    
    #Amalgamate Securitized and Securitized Bonds
    
    data['industry'] = data['industry'].replace({'Securitized':'Securitized Bonds'})
    
    #Amalgamate corporate bonds
    
    data['industry'] = data['industry'].replace({'Corporate':'Corporate Bonds',
                                             'Corporate Bonds/Securitized Bonds':'Corporate Bonds',
                                             'Corporate/Securitized':'Corporate Bonds',
                                             'Convertible Bonds':'Corporate Bonds'})
    
    #Amalgamate treasuries
    
    data['industry'] = data['industry'].replace({'Treasuries/Index Linked Bonds':'Treasuries',
                                             'Treasuries/Index Linked Bonds/Government Related Bonds':'Treasuries',
                                             'Treasuries/Government Related Bonds':'Treasuries'})
    
    #Create a list of all the treasuries
    
    list_of_treasuries = data['name'].loc[data['industry']== 'Treasuries'].unique().tolist()
    
    #If a bond name is in the list of treasuries then change the industry to treasuries
    
    data['industry'] = np.where((data['name'].isin(list_of_treasuries)),'Treasuries',data['industry'])
    
    #Amalgamate government bonds together
    
    data['industry'] = data['industry'].replace({'Government':'Government Bonds',
                                             'Government Related':'Government Bonds',
                                             'Government Related Bonds':'Government Bonds',
                                             'Government Related Bonds/Corporate Bonds':'Government Bonds',
                                             'Government Related Bonds/Securitized Bonds':'Government Bonds'})
    
    return data

#@task
def load_data(data):
       
    # Creates a connection string engine to upload a pandas dataframe to local postgres database
    
    secrets = st.secrets["postgres"]
    user = secrets["user"]
    password = secrets["password"]
    host = secrets["host"]
    port = secrets["port"]
    db_name = secrets["dbname"]
    
    connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    
    engine = create_engine(connection_str)
    
    #Uploads the pandas dataframe to local postgres database
      
    data.to_sql('oil_fund', engine, if_exists='replace', index=False)

#ETL process to be managed by Prefect
raw = extract_data()
transformed = transform_data(raw)
load_data(transformed)


""" #This portion is used for investigating and understanding the data wrangling steps

filepath = Path('C:/Users/rorya/Desktop/oil_fund_data_testing.xlsx')  
filepath.parent.mkdir(parents=True, exist_ok=True)
transformed.to_excel(filepath, index=False) """