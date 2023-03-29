from fuzzywuzzy import fuzz, process
import pandas as pd

class Transformations:
    
    def __init__(self):
        pass
    
    def merge_similar_strings(self, df, col):
        """
        Merges similar strings in a DataFrame column by finding the closest match using fuzzywuzzy.

        Args:
            df (pandas.DataFrame): The DataFrame containing the column to be merged.
            col (str): The name of the column to be merged.

        Returns:
            pandas.DataFrame: The modified DataFrame with merged strings.
        """

        merged_dict = {}

        for name in df[col].unique():

            # create a copy of the rows that have the same string in the column
            matches = df[df[col] == name].copy()

            # get the unique country and industry values for the matches
            country = matches['country'].unique()
            industry = matches['industry'].unique()

            # filter the original dataframe by country and industry and get the unique string values
            choices = df[df['country'].isin(country) & df['industry'].isin(industry)][col].unique()

            # find the closest match to the current string in the filtered choices using fuzzywuzzy extractOne
            merged_name = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio)[0]

            # update the name column in the original dataframe with the merged name for the rows that had the original name
            df.loc[df[col] == name, col] = merged_name

            # update the merged_dict with the original name and the merged name
            merged_dict[name] = merged_name

        return df
