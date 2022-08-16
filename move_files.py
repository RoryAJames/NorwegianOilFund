import os

target_folder = r'C:\Users\rorya\Desktop\Portfolio\Projects\NorwegianOilFund\data'

source_folder = r'C:\Users\rorya\Downloads\Oil_Fund_Data'

for path, dir, files in os.walk(source_folder):
    if files:
        for file in files:
            if not os.path.isfile(target_folder + file):
                os.rename(path + '\\' + file, target_folder + file)