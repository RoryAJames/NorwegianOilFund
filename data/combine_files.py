import os
import glob
import pandas as pd

os.chdir('C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/')

extension = 'csv'

all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

dfs_to_concat = []

for f in all_filenames:
    df = pd.read_csv(f)
    df['file_identifier'] = [f] * len(df.index)
    
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

new_col = {'Market Value(USD)':'Value'}
combined_csv.rename(columns= new_col, inplace = True)

#Change New Zealand in region to Oceania

#Create a CSV output of the combined data files

combined_csv.to_csv("data.csv", index=False)

#print(combined_csv.head())