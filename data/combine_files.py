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
    
    #Create investment category column
    
    #Create year column
    
    dfs_to_concat.append(df)
    
combined_csv = pd.concat(dfs_to_concat)

#combined_csv.to_csv("combined.csv", index=False)