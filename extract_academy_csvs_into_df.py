import pandas as pd
import glob
import os

def extract_academy_csvs_into_df():
    # Path to the folder containing the CSV files
    folder_path = 'raw_data/academy'

    # Get a list of all CSV files in the folder
    csv_files = glob.glob(folder_path + '/*.csv')

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Iterate through each CSV file and read it into the DataFrame
    for file in csv_files:
        data = pd.read_csv(file)

        # Extract the file name without the extension
        file_name = os.path.splitext(os.path.basename(file))[0]

        cohort_name = " ".join(file_name.split("_")[0:2])
        cohort_date = "".join(file_name.split("_")[2])

        data["Cohort Name"] = cohort_name
        data["Cohort Date"] = cohort_date

        df = pd.concat([df, data], ignore_index=True)

    return df


