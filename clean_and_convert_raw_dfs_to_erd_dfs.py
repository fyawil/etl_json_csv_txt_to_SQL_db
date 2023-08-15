from extract_into_dfs import academy_csvs_df, talent_csvs_df, talent_jsons_df, talent_txts_df

from clean_academy_csvs_df import clean_academy_csvs_df
from clean_talent_csvs_df import clean_talent_csvs_df
from clean_talent_jsons_df import clean_talent_jsons_df
from clean_talent_txts_df import clean_talent_txts_df

import pandas as pd

def rename_cols(df):
    for col in df.columns:
        df = df.rename(columns={col: col.split('_W')[0]})
    
    return df

# Cleaning the raw dataframes
cleaned_academy_csvs_df = clean_academy_csvs_df(academy_csvs_df)
cleaned_talent_csvs_df = clean_talent_csvs_df(talent_csvs_df)
cleaned_talent_jsons_df = clean_talent_jsons_df(talent_jsons_df)
cleaned_talent_txts_df = clean_talent_txts_df(talent_txts_df)

wide_academy_results_table = cleaned_academy_csvs_df.copy()

wide_academy_results_table.index.name = 'name_id'

week_one_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date'] 
week_two_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_three_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_four_cols =  ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_five_cols =  ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_six_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_seven_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_eight_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_nine_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']
week_ten_cols = ['name', 'trainer', 'Cohort Name', 'Cohort Date']

for col in wide_academy_results_table.columns:
    if 'W1' in col and 'W10' not in col:
        week_one_cols.append(col)
    if 'W2' in col:
        week_two_cols.append(col)
    if 'W3' in col:
        week_three_cols.append(col)
    if 'W4' in col:
        week_four_cols.append(col)
    if 'W5' in col:
        week_five_cols.append(col)
    if 'W6' in col:
        week_six_cols.append(col)
    if 'W7' in col:
        week_seven_cols.append(col)
    if 'W8' in col:
        week_eight_cols.append(col)
    if 'W9' in col:
        week_nine_cols.append(col)
    if 'W10' in col:
        week_ten_cols.append(col)

week_one_table = wide_academy_results_table[week_one_cols].copy()
week_one_table['week_no'] = 1

week_two_table = wide_academy_results_table[week_two_cols].copy()
week_two_table['week_no'] = 2

week_three_table = wide_academy_results_table[week_three_cols].copy()
week_three_table['week_no'] = 3

week_four_table = wide_academy_results_table[week_four_cols].copy()
week_four_table['week_no'] = 4

week_five_table = wide_academy_results_table[week_five_cols].copy()
week_five_table['week_no'] = 5

week_six_table = wide_academy_results_table[week_six_cols].copy()
week_six_table['week_no'] = 6

week_seven_table = wide_academy_results_table[week_seven_cols].copy()
week_seven_table['week_no'] = 7

week_eight_table = wide_academy_results_table[week_eight_cols].copy()
week_eight_table['week_no'] = 8

week_nine_table = wide_academy_results_table[week_nine_cols].copy()
week_nine_table['week_no'] = 9

week_ten_table = wide_academy_results_table[week_ten_cols].copy()
week_ten_table['week_no'] = 10

tables = [
    week_one_table,
    week_two_table,
    week_three_table,
    week_four_table,
    week_five_table,
    week_six_table,
    week_seven_table,
    week_eight_table,
    week_nine_table,
    week_ten_table
]

academy_results = rename_cols(week_one_table.copy())

for i, table in enumerate(tables):
    if i == 0:
        continue
    academy_results = pd.concat([academy_results, rename_cols(table)])

new_col_order = ['week_no', 'name', 'Cohort Name', 'Cohort Date', 'trainer', 'Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']

academy_results = academy_results[new_col_order]

###############################################################

academy = cleaned_academy_csvs_df.copy()

academy = academy[['Cohort Name']]

academy.index.name = 'name_id'

academy['Cohort Name'] = academy['Cohort Name'].map(lambda string: string.split(" ")[0])

course_name_to_id_dict = {
    'Business': 0,
    'Engineering': 1,
    'Data': 2
}

academy['Cohort Name'] = academy['Cohort Name'].map(course_name_to_id_dict)

academy = academy.rename(columns={'Cohort Name': 'course_id'})

###################################################

trainers_junc = cleaned_academy_csvs_df.copy()

trainers_junc = trainers_junc[['Cohort Name', 'trainer']]

trainers_junc['Cohort Name'] = trainers_junc['Cohort Name'].map(lambda string: string.split(" ")[0])
trainers_junc['Cohort Name'] = trainers_junc['Cohort Name'].map(course_name_to_id_dict)
trainers_junc = trainers_junc.rename(columns={'Cohort Name': 'course_id'})

trainer_name_to_id_dict = {}
for i, trainer in enumerate(trainers_junc['trainer'].unique()):
    trainer_name_to_id_dict[trainer] = i

trainers_junc['trainer'] = trainers_junc['trainer'].map(trainer_name_to_id_dict)
trainers_junc = trainers_junc.rename(columns={'trainer': 'trainer_id'})

trainers_junc = trainers_junc.drop_duplicates()

trainers_junc = trainers_junc.reset_index(drop=True)

trainers_junc = trainers_junc.sort_values(by=['course_id', 'trainer_id'])

###################################

trainers = pd.DataFrame(trainer_name_to_id_dict.items(), columns=['trainer_name', 'trainer_id'])

trainers = trainers[['trainer_id', 'trainer_name']]

######################

courses = pd.DataFrame(course_name_to_id_dict.items(), columns=['course_name', 'course_id'])

courses = courses[['course_id', 'course_name']]

#######################

# Extracting cols needed for candidate table
candidate = cleaned_talent_csvs_df.copy()

candidate = candidate[['name', 'gender', 'dob', 'email', 'city', 'address', 'postcode',
       'phone_number', 'uni', 'degree', 'invited_date_day',
       'invited_date_month_and_year', 'invited_by']]

# Adding name_id to candidate table
name_id_list = []

name_list_candidate = list(candidate['name'])

current_new_id = 397
for name in name_list_candidate:
    if name in list(cleaned_academy_csvs_df['name']):
        name_id_list.append(cleaned_academy_csvs_df['name'][cleaned_academy_csvs_df['name'] == name].index[0])
    else:
        name_id_list.append(current_new_id)
        current_new_id += 1

candidate['name_id'] = name_id_list

print(cleaned_academy_csvs_df[cleaned_academy_csvs_df['name'] == 'MATTHAEUS AUDAS'])

