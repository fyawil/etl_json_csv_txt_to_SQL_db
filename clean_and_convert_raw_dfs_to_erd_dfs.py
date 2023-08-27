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

cleaned_parent_df = cleaned_academy_csvs_df.merge(
    right=cleaned_talent_csvs_df, how='outer', on='name')
cleaned_parent_df = cleaned_parent_df.merge(
    right=cleaned_talent_jsons_df, how='outer', on='name')
cleaned_parent_df = cleaned_parent_df.merge(
    right=cleaned_talent_txts_df, how='outer', on='name')

# Creating the name_id column

cleaned_parent_df['name_id'] = cleaned_parent_df.index

# Creating academy table

academy = cleaned_parent_df.copy()

academy = academy[['name_id', 'Cohort Name']]

academy['Cohort Name'] = academy['Cohort Name'].map(
    lambda course: course.split(' ')[0], na_action='ignore')

academy['course_id'] = academy['Cohort Name'].map(
    {'Business': 0, 'Engineering': 1, 'Data': 2})

academy = academy[['name_id', 'course_id']]

# Creating course table

course = pd.DataFrame(data={'course_id': [0, 1, 2], 'course_name': [
                      'Business', 'Engineering', 'Data']})

# Creating trainer table

unique_trainers = (cleaned_parent_df.copy())['trainer'].dropna().unique()

trainer = pd.DataFrame(data=unique_trainers, columns=['trainer_name'])
trainer['trainer_id'] = trainer.index

# Creating trainer_junction table

trainer_junction = cleaned_parent_df.copy()

trainer_junction = trainer_junction[['trainer', 'Cohort Name']]

trainer_junction.dropna(inplace=True)

trainer_junction.drop_duplicates(inplace=True)

trainer_junction['Cohort Name'] = trainer_junction['Cohort Name'].map(
    lambda course: course.split(' ')[0], na_action='ignore')
trainer_junction['Cohort Name'] = trainer_junction['Cohort Name'].map(
    {'Business': 0, 'Engineering': 1, 'Data': 2})
trainer_junction.rename(columns={'Cohort Name': 'course_id'})


trainer_to_trainer_id_dict = dict(
    zip(trainer['trainer_name'], trainer['trainer_id']))
trainer_junction['trainer'] = trainer_junction['trainer'].map(
    trainer_to_trainer_id_dict)
trainer_junction.rename(columns={'trainer': 'trainer_id'})

trainer_junction.reset_index(inplace=True, drop=True)

# Creating academy_result table

academy_result_wide = cleaned_parent_df.copy()

academy_result_wide = academy_result_wide[['name', 'Analytic_W1', 'Independent_W1', 'Determined_W1',
                                           'Professional_W1', 'Studious_W1', 'Imaginative_W1', 'Analytic_W2',
                                           'Independent_W2', 'Determined_W2', 'Professional_W2', 'Studious_W2',
                                           'Imaginative_W2', 'Analytic_W3', 'Independent_W3', 'Determined_W3',
                                           'Professional_W3', 'Studious_W3', 'Imaginative_W3', 'Analytic_W4',
                                           'Independent_W4', 'Determined_W4', 'Professional_W4', 'Studious_W4',
                                           'Imaginative_W4', 'Analytic_W5', 'Independent_W5', 'Determined_W5',
                                           'Professional_W5', 'Studious_W5', 'Imaginative_W5', 'Analytic_W6',
                                           'Independent_W6', 'Determined_W6', 'Professional_W6', 'Studious_W6',
                                           'Imaginative_W6', 'Analytic_W7', 'Independent_W7', 'Determined_W7',
                                           'Professional_W7', 'Studious_W7', 'Imaginative_W7', 'Analytic_W8',
                                           'Independent_W8', 'Determined_W8', 'Professional_W8', 'Studious_W8',
                                           'Imaginative_W8', 'Analytic_W9',
                                           'Independent_W9', 'Determined_W9', 'Professional_W9', 'Studious_W9',
                                           'Imaginative_W9', 'Analytic_W10', 'Independent_W10', 'Determined_W10',
                                           'Professional_W10', 'Studious_W10', 'Imaginative_W10']]

academy_result_w1 = academy_result_wide[['name', 'Analytic_W1', 'Independent_W1', 'Determined_W1',
                                         'Professional_W1', 'Studious_W1', 'Imaginative_W1']]
academy_result_w2 = academy_result_wide[['name', 'Analytic_W2', 'Independent_W2', 'Determined_W2',
                                         'Professional_W2', 'Studious_W2', 'Imaginative_W2']]
academy_result_w3 = academy_result_wide[['name', 'Analytic_W3', 'Independent_W3', 'Determined_W3',
                                         'Professional_W3', 'Studious_W3', 'Imaginative_W3']]
academy_result_w4 = academy_result_wide[['name', 'Analytic_W4', 'Independent_W4', 'Determined_W4',
                                         'Professional_W4', 'Studious_W4', 'Imaginative_W4']]
academy_result_w5 = academy_result_wide[['name', 'Analytic_W5', 'Independent_W5', 'Determined_W5',
                                         'Professional_W5', 'Studious_W5', 'Imaginative_W5']]
academy_result_w6 = academy_result_wide[['name', 'Analytic_W6', 'Independent_W6', 'Determined_W6',
                                         'Professional_W6', 'Studious_W6', 'Imaginative_W6']]
academy_result_w7 = academy_result_wide[['name', 'Analytic_W7', 'Independent_W7', 'Determined_W7',
                                         'Professional_W7', 'Studious_W7', 'Imaginative_W7']]
academy_result_w8 = academy_result_wide[['name', 'Analytic_W8', 'Independent_W8', 'Determined_W8',
                                         'Professional_W8', 'Studious_W8', 'Imaginative_W8']]
academy_result_w9 = academy_result_wide[['name', 'Analytic_W9', 'Independent_W9', 'Determined_W9',
                                         'Professional_W9', 'Studious_W9', 'Imaginative_W9']]
academy_result_w10 = academy_result_wide[['name', 'Analytic_W10', 'Independent_W10', 'Determined_W10',
                                          'Professional_W10', 'Studious_W10', 'Imaginative_W10']]

def add_week_no(df):
    df['week_no'] = int(df.columns[1].split('_W')[1])

def remove_week_no_from_cols(df):
    df.rename(columns=lambda col: col.split('_W')[0], inplace = True)

academy_results_sep_into_weeks = [academy_result_w1,
academy_result_w2,
academy_result_w3,
academy_result_w4,
academy_result_w5,
academy_result_w6,
academy_result_w7,
academy_result_w8,
academy_result_w9,
academy_result_w10]

for weekly_table in academy_results_sep_into_weeks:
    add_week_no(weekly_table)
    remove_week_no_from_cols(weekly_table)

academy_result = pd.concat(academy_results_sep_into_weeks)

academy_result.dropna(inplace=True, thresh=6)

academy_result.reset_index(inplace=True, drop=True)

name_to_name_id_dict = dict(
    zip(cleaned_parent_df['name'], cleaned_parent_df['name_id']))
academy_result['name'] = academy_result['name'].map(
    name_to_name_id_dict)
academy_result.rename(inplace=True, columns={'name': 'name_id'})

academy_result.rename(inplace=True, columns=lambda col: col.lower())

print(academy_result)