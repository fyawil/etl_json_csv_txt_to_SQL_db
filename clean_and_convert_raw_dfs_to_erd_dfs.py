from extract_into_dfs import academy_csvs_df, talent_csvs_df, talent_jsons_df, talent_txts_df

from clean_academy_csvs_df import clean_academy_csvs_df
from clean_talent_csvs_df import clean_talent_csvs_df
from clean_talent_jsons_df import clean_talent_jsons_df
from clean_talent_txts_df import clean_talent_txts_df

import pandas as pd

# Creating four clean df's

cleaned_academy_csvs_df = clean_academy_csvs_df(academy_csvs_df)
cleaned_talent_csvs_df = clean_talent_csvs_df(talent_csvs_df)
cleaned_talent_jsons_df = clean_talent_jsons_df(talent_jsons_df)
cleaned_talent_txts_df = clean_talent_txts_df(talent_txts_df)

# Renaming talent_txts_df col to match the rest for merging

cleaned_talent_txts_df.rename(columns={'Name': 'name'}, inplace=True)

# Merging the four on name to create a parent df of cleaned data

cleaned_parent_df = cleaned_academy_csvs_df.merge(right=cleaned_talent_csvs_df,how='outer',on='name')
cleaned_parent_df = cleaned_parent_df.merge(right=cleaned_talent_jsons_df,how='outer',on='name')
cleaned_parent_df = cleaned_parent_df.merge(right=cleaned_talent_txts_df,how='outer',on='name')

# Creating the name_id column

cleaned_parent_df['name_id'] = cleaned_parent_df.index

# Creating academy table

academy = cleaned_parent_df.copy()

academy = academy[['name_id', 'Cohort Name']]

academy['Cohort Name'] = academy['Cohort Name'].map(lambda course: course.split(' ')[0], na_action='ignore')

academy['course_id'] = academy['Cohort Name'].map({'Business': 0, 'Engineering': 1, 'Data': 2})

academy = academy[['name_id', 'course_id']]

# Creating course table

course = pd.DataFrame(data={'course_id': [0, 1, 2], 'course_name': ['Business', 'Engineering', 'Data']})

print(course)