import os
import glob

path = 'C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/'
for f in glob.iglob(path+'/**/*.xlsx', recursive=True):
    os.remove(f)