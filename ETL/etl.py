import os
import glob
import pandas as pd
import numpy as np
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
        df['Category'] = df['file_identifier'].apply(lambda x: 'Equity' if x[:2] == 'EQ' else 'Fixed Income')
        
        #Create year column from file identifier
        df['Year'] = df['file_identifier'].str[3:7].astype(int)
        
        dfs_to_concat.append(df)
    
    # Concat dataframes together to create a combined dataframe
    df = pd.concat(dfs_to_concat)
    
    return df

#A function that performs all of the data cleaning tasks before loading into the database.

#@task
def transform_data():

    df = extract_data()
    
    #Drop unwanted columns
    
    cols_to_drop = ['Market Value(NOK)','Voting','Ownership','Incorporation Country','file_identifier']

    df.drop(columns=cols_to_drop, inplace=True)
    
    #Rename market value column to 'Value'

    new_col = {'Market Value(USD)':'Market_Value'}
    df.rename(columns= new_col, inplace = True)
    
    #Assign countries to their appropriate regions 

    df['Region'] = df['Region'].replace({'Australia':'Oceania',
                                         'New Zealand':'Oceania',
                                         'Japan':'Asia'})
    
    #Fix spelling of various countries

    df['Country'] = df['Country'].replace({'Faeroe Islands':'Faroe Islands',
                                           'Guernsey C. I.':'Guernsey',
                                           'Gurensey':'Guernsey',
                                           'Jersey C.I.':'Jersey',
                                           'Lichtenstein':'Liechtenstein',
                                           'Tanzania *, United Republic of':'Tanzania'})
    
    # Fix Guernsey in industry

    df['Industry'] = df['Industry'].replace({'Guernsey':'Financials'})

    # Fix unknown in industry, assign the values based on company name

    df['Industry'] = np.where((df['Industry'] == 'Unknown') & (df['Name'] == 'Craft Oil Ltd'), 'Energy',
                     np.where((df['Industry'] == 'Unknown') & (df['Name'] == 'Kontron S&T AG'), 'Technology',
                               df['Industry']))
    
    #Assign consumer services to discretionary and consumer goods to consumer staples
    
    df['Industry'] = df['Industry'].replace({'Consumer Services':'Consumer Discretionary',
                                             'Consumer Goods':'Consumer Staples'})
    
    #Assign oil and gas to energy
    
    df['Industry'] = df['Industry'].replace({'Oil & Gas':'Energy'})
    
    #Create a list of all the unique real estate companies.
    
    real_estate_companies = df['Name'].loc[df['Industry']== 'Real Estate'].unique().tolist()
    
    # Up until 2020 real estate companies were categorized as financials. If a company name is in the real estate list change the industry to real estate.
    
    df['Industry'] = np.where((df['Name'].isin(real_estate_companies)),'Real Estate',df['Industry'])
    
    #If a company is in real estate and fixed income change the industry to corporate bonds
    
    df['Industry'] = np.where((df['Industry'] == 'Real Estate') & (df['Category'] == 'Fixed Income'), 'Corporate Bonds',df['Industry'])
    
    #Amalgamate Securitized and Securitized Bonds
    
    df['Industry'] = df['Industry'].replace({'Securitized':'Securitized Bonds'})
    
    #Amalgamate corporate bonds
    
    df['Industry'] = df['Industry'].replace({'Corporate':'Corporate Bonds',
                                             'Corporate Bonds/Securitized Bonds':'Corporate Bonds',
                                             'Corporate/Securitized':'Corporate Bonds',
                                             'Convertible Bonds':'Corporate Bonds'})
    
    #Amalgamate treasuries
    
    df['Industry'] = df['Industry'].replace({'Treasuries/Index Linked Bonds':'Treasuries',
                                             'Treasuries/Index Linked Bonds/Government Related Bonds':'Treasuries',
                                             'Treasuries/Government Related Bonds':'Treasuries'})
    
    #Create a list of all the treasuries
    
    list_of_treasuries = df['Name'].loc[df['Industry']== 'Treasuries'].unique().tolist()
    
    #If a bond name is in the list of treasuries then change the industry to treasuries
    
    df['Industry'] = np.where((df['Name'].isin(list_of_treasuries)),'Treasuries',df['Industry'])
    
    #Amalgamate government bonds together
    
    df['Industry'] = df['Industry'].replace({'Government':'Government Bonds',
                                             'Government Related':'Government Bonds',
                                             'Government Related Bonds':'Government Bonds',
                                             'Government Related Bonds/Corporate Bonds':'Government Bonds',
                                             'Government Related Bonds/Securitized Bonds':'Government Bonds'})
    
    return df

#@task
def load_data():
    pass

#Load transformed data
test = transform_data()

#Send a test csv file to desktop for easier investigating

filepath = Path('C:/Users/rorya/Desktop/oil_fund_data_testing.csv')  
filepath.parent.mkdir(parents=True, exist_ok=True)
test.to_csv(filepath, index=False)