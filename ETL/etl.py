import os
import glob
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, VARCHAR, BIGINT, NUMERIC, INTEGER
from pathlib import Path
from transform import Transformations

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
    
    print("Files extracted.")
    
    return df


def transform_data(data):

    #Drop unwanted columns
    
    cols_to_drop = ['Market Value(NOK)','Voting','Incorporation Country','file_identifier']

    data.drop(columns=cols_to_drop, inplace=True)
    
    #Rename columns

    new_col = {'Market Value(USD)':'market_value',
               'Ownership':'percent_ownership',
               'Industry':'sector'}
      
    data.rename(columns= new_col, inplace = True)
    
    #Make all of the columns lowercase so that you can properly query the database in postgres
    
    data.columns = data.columns.str.lower()
    
    #Fill the percent ownership null values in fixed income with 0
    
    data['percent_ownership'] = data['percent_ownership'].fillna(0)
    
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
    
    # Fix Guernsey in sector

    data['sector'] = data['sector'].replace({'Guernsey':'Financials'})

    # Fix unknown in sector, assign the values based on company name

    data['sector'] = np.where((data['sector'] == 'Unknown') & (data['name'] == 'Craft Oil Ltd'), 'Energy',
                                np.where((data['sector'] == 'Unknown') & (data['name'] == 'Kontron S&T AG'), 'Technology',
                                         data['sector']))
    
    #Assign consumer services to discretionary and consumer goods to consumer staples
    
    data['sector'] = data['sector'].replace({'Consumer Services':'Consumer Discretionary',
                                             'Consumer Goods':'Consumer Staples'})
    
    #Assign oil and gas to energy
    
    data['sector'] = data['sector'].replace({'Oil & Gas':'Energy'})
    
    #Create a list of all the unique real estate companies.
    
    real_estate_companies = data['name'].loc[data['sector']== 'Real Estate'].unique().tolist()
    
    # Up until 2020 real estate companies were categorized as financials. If a company name is in the real estate list change the sector to real estate.
    
    data['sector'] = np.where((data['name'].isin(real_estate_companies)),'Real Estate',data['sector'])
    
    #If a company is in real estate and fixed income change the sector to corporate bonds
    
    data['sector'] = np.where((data['sector'] == 'Real Estate') & (data['category'] == 'Fixed Income'), 'Corporate Bonds',data['sector'])
    
    #Amalgamate Securitized and Securitized Bonds
    
    data['sector'] = data['sector'].replace({'Securitized':'Securitized Bonds'})
    
    #Amalgamate corporate bonds
    
    data['sector'] = data['sector'].replace({'Corporate':'Corporate Bonds',
                                             'Corporate Bonds/Securitized Bonds':'Corporate Bonds',
                                             'Corporate/Securitized':'Corporate Bonds',
                                             'Convertible Bonds':'Corporate Bonds'})
    
    #Amalgamate treasuries
    
    data['sector'] = data['sector'].replace({'Treasuries/Index Linked Bonds':'Treasuries',
                                             'Treasuries/Index Linked Bonds/Government Related Bonds':'Treasuries',
                                             'Treasuries/Government Related Bonds':'Treasuries'})
    
    #Create a list of all the treasuries
    
    list_of_treasuries = data['name'].loc[data['sector']== 'Treasuries'].unique().tolist()
    
    #If a bond name is in the list of treasuries then change the sector to treasuries
    
    data['sector'] = np.where((data['name'].isin(list_of_treasuries)),'Treasuries',data['sector'])
    
    #Amalgamate government bonds together
    
    data['sector'] = data['sector'].replace({'Government':'Government Bonds',
                                             'Government Related':'Government Bonds',
                                             'Government Related Bonds':'Government Bonds',
                                             'Government Related Bonds/Corporate Bonds':'Government Bonds',
                                             'Government Related Bonds/Securitized Bonds':'Government Bonds'})
    
    trans = Transformations()
    
    print("Merging similar strings.")
    
    data = trans.merge_similar_strings(data,'name')
    
    print("Transformations complete.")
    
    return data


def load_data(data):
       
    # Creates a connection string engine to upload a pandas dataframe to postgres database
    
    secrets = st.secrets["postgres"]
    user = secrets["user"]
    password = secrets["password"]
    host = secrets["host"]
    port = secrets["port"]
    db_name = secrets["dbname"]
    
    connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    
    engine = create_engine(connection_str)
    
    print("Database connection established.")
    
    #Set the data types of the dataframe columns to be uploaded to postgres
    
    df_schema = {
    'region': VARCHAR(100),
    'country': VARCHAR(100),
    'name': VARCHAR(100),
    'sector': VARCHAR(100),
    'market_value': BIGINT,
    'percent_ownership': NUMERIC,
    'category': VARCHAR(100),
    'year': INTEGER}
    
    #Uploads the pandas dataframe to postgres database
      
    data.to_sql('oil_fund', engine, if_exists='replace', index=False, dtype = df_schema, chunksize = 1000)
    
    print("Data loaded into Postgres.")


#Manual ETL process
raw = extract_data()
transformed = transform_data(raw)
load_data(transformed)

print("ETL process complete.")


#Output dataframes as excel files to desktop for further investigating

# def make_excel_file(file,filename):
#     filepath = Path(f'C:/Users/rorya/Desktop/{filename}.xlsx')  
#     filepath.parent.mkdir(parents=True, exist_ok=True)
#     file.to_excel(filepath, index=False)


# make_excel_file(transformed,'current_oil_fund_DB')