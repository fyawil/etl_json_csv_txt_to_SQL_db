import pandas as pd

from extract_into_dfs import talent_jsons_df

def clean_talent_jsons_df(df):
    # Convert list columns strengths and weaknesses to strings
    df['strengths'] = df['strengths'].apply(lambda x: str(x))
    df['weaknesses'] = df['weaknesses'].apply(lambda x: str(x))

    # Dropping duplicate rows
    df = df.drop_duplicates()

    # Replacing Yes/No with True/False in 'self_development'
    df.loc[:, 'self_development'] = df['self_development'].map({'Yes': True, 'No': False})

    # Replacing Yes/No with True/False in 'geo_flex'
    df.loc[:, 'geo_flex'] = df['geo_flex'].map({'Yes': True, 'No': False})

    # Replacing Yes/No with True/False in 'financial_support_self'
    df.loc[:, 'financial_support_self'] = df['financial_support_self'].map({'Yes': True, 'No': False})

    # Casting all floats in self_score columns to integers

    columns_to_convert = ['tech_self_score.C#',
                        'tech_self_score.Java',
                        'tech_self_score.R',
                        'tech_self_score.JavaScript',
                        'tech_self_score.Python',
                        'tech_self_score.C++',
                        'tech_self_score.Ruby',
                        'tech_self_score.SPSS',
                        'tech_self_score.PHP']

    float_to_int_dict = {
        1.0: 1,
        2.0: 2,
        3.0: 3,
        4.0: 4,
        5.0: 5,
        6.0: 6,
        7.0: 7,
        8.0: 8,
        9.0: 9,
        10.0: 10,
        11.0: 11,
        12.0: 12
    }

    # Use the map() method with the float_to_int_dict to convert floats to integers
    for col in columns_to_convert:
        df[col] = df[col].map(float_to_int_dict).astype('Int64')
    
    return df