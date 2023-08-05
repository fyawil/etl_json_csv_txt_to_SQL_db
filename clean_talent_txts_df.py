import pandas as pd

def clean_talent_txts_df(df):
    # Dropping duplicate rows to leave the latest Sparta Day for each person in the dataset
    df.drop(index=[1500, 3239, 3472], inplace=True)

    # Casting Sparta Day Date to datetime format
    df['Sparta Day Date'] = pd.to_datetime(df['Sparta Day Date'])

    # Converting Name column to upper case for consistency
    df['Name'] = df['Name'].str.strip().str.upper()

    # Casting Presentation Score to integers
    df['Presentation Score'] = df['Presentation Score'].astype(int)

    # Casting Presentation Score to integers
    df['Psychometrics Score'] = df['Psychometrics Score'].astype(int)

    return df

from extract_into_dfs import talent_txts_df

print(clean_talent_txts_df(talent_txts_df))