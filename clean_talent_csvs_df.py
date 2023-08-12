import pandas as pd
import re

def clean_talent_csvs_df(df):

    # Create an independent copy of the DataFrame
    df = df.copy()

    # Converting name column to upper case for consistency
    df['name'] = df['name'].str.strip().str.upper()

    # Dropping the id column as it is not unique
    df = df.drop('id', axis=1)

    # Changing Female to F and Male to M
    df['gender'] = df['gender'].str[0]
    
    # Casting dob to datetime
    df['dob'] = pd.to_datetime(df['dob'], format='mixed')

    # Stripping whitespace from either side of email values
    df['email'] = df['email'].str.strip()

    # Stripping whitespace from either side of city values
    df['city'] = df['city'].str.strip()

    # Removing leading zero from address values for consistency
    df['address'] = df['address'].str.strip().str.lstrip('0')

    # Removing whitespace from either side of postcode values
    df['postcode'] = df['postcode'].str.strip()

    # Replacing everything except 0-9 and + with nothing leaving number in format +44##########
    unwanted_chars = ['-', ' ', ')', '(']
    pattern = '[' + re.escape(''.join(unwanted_chars)) + ']'
    df['phone_number'] = df['phone_number'].str.replace(pattern, '', regex=True)

    # Removing white space either side of value in cells of uni
    df['uni'] = df['uni'].str.strip()

    # Since invited date columns are misnamed, we will rename the columns
    df = df.rename(columns={'invited_date': 'invited_date_day', 'month': 'invited_date_month_and_year'})

    # Casting invited_day_date to integers
    df['invited_date_day'] = df['invited_date_day'].astype(pd.Int64Dtype(), errors='ignore')

    # Updating the values for consistency
    update_dict = {
        'April 2019': '2019-04',
        'AUGUST 2019': '2019-08', 
        'DECEMBER 2019': '2019-12', 
        'February 2019': '2019-02',
        'January 2019': '2019-01', 
        'JULY 2019': '2019-07', 
        'JUNE 2019': '2019-06', 
        'March 2019': '2019-03', 
        'May 2019': '2019-05',
        'NOVEMBER 2019': '2019-11', 
        'OCTOBER 2019': '2019-10', 
        'SEPT 2019': '2019-09'
    }
    df['invited_date_month_and_year'] = df['invited_date_month_and_year'].replace(update_dict)

    # Updating correcting misspelt Bruno Belbrook to Bruno Bellbrook
    df['invited_by'] = df['invited_by'].replace({'Bruno Belbrook': 'Bruno Bellbrook'})

    return df