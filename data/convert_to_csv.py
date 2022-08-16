import csv
import pandas as pd
import os
import glob

directory = 'C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/'

for xlsx_file in glob.glob(os.path.join(directory,"*.xlsx")):
    data_xlsx = pd.read_excel(xlsx_file)
    csv_file = os.path.splitext(xlsx_file)[0]+".csv"
    data_xlsx.to_csv(csv_file, encoding='utf-8',index=False)