import pandas as pd
import json
import glob
import os

def extract_talent_jsons_into_df():

    # Path to the folder containing the JSON files
    folder_path = 'raw_data/talent'

    # Get a list of all JSON files in the folder
    json_files = glob.glob(folder_path + '/*.json')

    # Initialize an empty list to store the data
    data_list = []

    # Iterate through each JSON file
    for file in json_files:
        with open(file, 'r') as f:
            # Load the JSON data
            json_data = json.load(f)
            # Append the JSON data to the list
            data_list.append(json_data)

    # Create a DataFrame from the list of JSON data
    df = pd.json_normalize(data_list)

    return df


