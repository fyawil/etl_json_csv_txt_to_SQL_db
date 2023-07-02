import pandas as pd
import glob

def extract_talent_csvs_into_df():
    # Path to the folder containing the CSV files
    folder_path = 'raw_data/talent'

    # Get a list of all CSV files in the folder
    csv_files = glob.glob(folder_path + '/*.csv')

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Iterate through each CSV file and read it into the DataFrame
    for file in csv_files:
        data = pd.read_csv(file)

        df = pd.concat([df, data], ignore_index=True)

    return df


