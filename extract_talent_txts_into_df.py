import pandas as pd
import json
import glob
import os

def extract_talent_txts_into_df():

    # Path to the folder containing the JSON files
    folder_path = 'raw_data/talent'

    # Get a list of all TXT files in the folder
    txt_files = glob.glob(folder_path + '/*.txt')

    headers = ["Sparta Day Date", "Academy", "Name", "Psychometrics Score", "Presentation Score"]

    # Initialize an empty list to store the data
    data_list = []

    # Iterate through each TXT file
    for file in txt_files:
        with open(file, 'r') as f:
            lines = f.readlines()
            sparta_day_date = lines[0].strip()
            academy = lines[1].split(" ")[0]
            for line in lines[3:]:
                if len(line.split("-")) > 2:
                    name = "-".join(line.split("-")[0:2]).strip()
                else:
                    name = line.split("-")[0].strip()
                psych_score = line.split(":")[1].split("/")[0].strip()
                pres_score = line.split(":")[2].split("/")[0].strip()

                data_list.append([sparta_day_date, academy, name, psych_score,pres_score])

    # Create a DataFrame from the list of JSON data
    df = pd.DataFrame(data_list, columns=headers)

    return df


