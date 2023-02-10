import os
import glob
import pandas as pd
import numpy as np

os.chdir('C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/')

extension = 'csv'

all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

dfs_to_concat = []

for f in all_filenames:
    df = pd.read_csv(f, thousands=r',') #Removes thousands separator when reading in CSV file. This ensures the values are integers
    
    df['file_identifier'] = [f] * len(df.index) #Creates a file identifier for each csv file
    
    #Create investment category column by extracting the first two characters from file identifier
    df['Category'] = df['file_identifier'].apply(lambda x: 'Equity' if x[:2] == 'EQ' else 'Fixed Income')
    
    #Create year column from file identifier
    df['Year'] = df['file_identifier'].str[3:7].astype(int)
    
    dfs_to_concat.append(df)
 
# Concat dataframes together to create the master
combined_csv = pd.concat(dfs_to_concat)

#Remove unwanted columns

cols_to_drop = ['Market Value(NOK)','Voting','Ownership','Incorporation Country','file_identifier']

combined_csv.drop(columns=cols_to_drop, inplace=True)

#Rename market value column to 'Value'

new_col = {'Market Value(USD)':'Market_Value'}
combined_csv.rename(columns= new_col, inplace = True)

#Assign countries to their appropriate regions 

combined_csv['Region'] = combined_csv['Region'].replace({'Australia':'Oceania',
                                                         'New Zealand':'Oceania',
                                                         'Japan':'Asia'})

#Fix spelling of various countries

combined_csv['Country'] = combined_csv['Country'].replace({'Faeroe Islands':'Faroe Islands',
                                                           'Guernsey C. I.':'Guernsey',
                                                           'Gurensey':'Guernsey',
                                                           'Jersey C.I.':'Jersey',
                                                           'Lichtenstein':'Liechtenstein',
                                                           'Tanzania *, United Republic of':'Tanzania'})

# Fix Guernsey in industry

combined_csv['Industry'] = combined_csv['Industry'].replace({'Guernsey':'Financials'})

# Fix unknown in industry, assign the values based on company name

combined_csv['Industry'] = np.where((combined_csv['Industry'] == 'Unknown') & (combined_csv['Name'] == 'Craft Oil Ltd'), 'Energy',
                           np.where((combined_csv['Industry'] == 'Unknown') & (combined_csv['Name'] == 'Kontron S&T AG'), 'Technology',
                                    combined_csv['Industry']))

#Create a CSV output of the combined data files

combined_csv.to_csv("data.csv", index=False)

#print(combined_csv.head())