import os

source_folder = 'C:/Users/rorya/Downloads/Oil_Fund_Data/'

target_folder = 'C:/Users/rorya/Desktop/Portfolio/Projects/NorwegianOilFund/data/'

all_files = os.listdir(source_folder)

for file in all_files:
    os.rename(source_folder + file, target_folder + file)