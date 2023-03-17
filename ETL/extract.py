import os
import glob
import pandas as pd

'''A function that extracts all of the csv files from the data folder, creates a unique identifier for each CSV file, extracts the investment category and year, 
then combines the files together creating the master dataframe'''  

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