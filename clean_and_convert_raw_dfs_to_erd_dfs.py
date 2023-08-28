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

course_id_mapping = {'Business': 0, 'Engineering': 1, 'Data': 2}
academy['course_id'] = academy['Cohort Name'].map(course_id_mapping)

academy = academy[['name_id', 'course_id']]

academy.dropna(how='any', inplace=True)

# Creating course table

course = pd.DataFrame(data={'course_id': [0, 1, 2], 'course_name': [
                      'Business', 'Engineering', 'Data']})

# Creating trainer table

unique_trainers = (cleaned_parent_df.copy())['trainer'].dropna().unique()

trainer = pd.DataFrame(data=unique_trainers, columns=['trainer_name'])
trainer['trainer_id'] = trainer.index

trainer = trainer[['trainer_id', 'trainer_name']]

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
trainer_junction.rename(columns={'trainer': 'trainer_id', 'Cohort Name': 'course_id'}, inplace=True)

trainer_junction = trainer_junction[['course_id', 'trainer_id']]

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
def add_week_no(df):
    new_df = df.copy()
    week_col = new_df.columns[1]
    week_no = int(week_col.split('_W')[1])
    new_df['week_no'] = week_no
    return new_df


for idx, weekly_table in enumerate(academy_results_sep_into_weeks):
    academy_results_sep_into_weeks[idx] = add_week_no(weekly_table)

def remove_suffix_after_w(df):
    new_columns = [col.split('_W')[0] if '_W' in col else col for col in df.columns]
    df.columns = new_columns
    return df

for idx, weekly_table in enumerate(academy_results_sep_into_weeks):
    academy_results_sep_into_weeks[idx] = remove_suffix_after_w(weekly_table)


academy_result = pd.concat(academy_results_sep_into_weeks)

academy_result.dropna(inplace=True, thresh=6)

name_to_name_id_dict = dict(
    zip(cleaned_parent_df['name'], cleaned_parent_df['name_id']))
academy_result['name'] = academy_result['name'].map(
    name_to_name_id_dict)

academy_result.rename(columns={'name': 'name_id'}, inplace=True)
academy_result.rename(columns=str.lower, inplace=True)

academy_result = academy_result[['name_id', 'week_no', 'analytic', 'independent', 'determined', 'professional',
       'studious', 'imaginative']]

# Creating the candidate table

candidate = cleaned_parent_df.copy()

candidate = candidate[['name', 'name_id', 'date', 'invited_by', 'city', 'address', 'postcode', 'gender', 'dob',
                       'email', 'phone_number', 'degree', 'self_development', 'result', 'geo_flex',
                       'financial_support_self', 'course_interest', 'Sparta Day Date',
                       'Psychometrics Score', 'Presentation Score', 'Academy']]

candidate.rename(inplace=True, columns={'date': 'interview_date', 'dob': 'date_of_birth', 'degree': 'uni_degree', 'result': 'interview_result', 'geo_flex': 'geoflex', 'Sparta Day Date': 'sparta_day_date',
                                        'Psychometrics Score': 'psychometric_result', 'Presentation Score': 'presentation_result', 'Academy': 'sparta_day_location'})

candidate['dollarsign_joined_address'] = candidate['address'] + \
    "$" + candidate['city'] + "$" + candidate['postcode']

candidate.drop(columns=['address', 'city', 'postcode'], inplace=True)

unique_candidate_addresses = candidate['dollarsign_joined_address'].unique()

full_address_to_address_id_df = pd.DataFrame(
    data=unique_candidate_addresses, columns=['address'])

full_address_to_address_id_df['address_id'] = full_address_to_address_id_df.index

address_to_id_dict = dict(zip(
    full_address_to_address_id_df['address'], full_address_to_address_id_df['address_id']))

candidate['dollarsign_joined_address'] = candidate['dollarsign_joined_address'].map(
    address_to_id_dict)

candidate.rename(inplace=True, columns={
                 'dollarsign_joined_address': 'address_id'})

candidate = candidate[['name', 'name_id', 'interview_date', 'invited_by',
       'address_id', 'gender', 'date_of_birth', 'email', 'phone_number',
       'uni_degree', 'self_development', 'interview_result', 'geoflex',
       'financial_support_self', 'course_interest', 'sparta_day_date',
       'psychometric_result', 'presentation_result', 'sparta_day_location']]

# Creating the address table

address = cleaned_parent_df.copy()

address = address[['address', 'city', 'postcode']]

address['dollarsign_joined_address'] = address['address'] + \
    "$" + address['city'] + "$" + address['postcode']

address['dollarsign_joined_address'] = address['dollarsign_joined_address'].map(
    address_to_id_dict)

address.rename(inplace=True, columns={
               'dollarsign_joined_address': 'address_id'})

address.drop_duplicates(inplace=True)


address = address[['address_id', 'address', 'city', 'postcode']]

# Creating the strength table

strength = (cleaned_parent_df.copy())

strength = strength[['strengths']]

strength.dropna(inplace=True)

strength['strengths'] = strength['strengths'].apply(lambda x: x.strip('['))
strength['strengths'] = strength['strengths'].apply(lambda x: x.strip(']'))

string_strengths = ", ".join(list(strength['strengths'])).replace("'", "")

list_of_strengths = string_strengths.split(", ")

set_of_strengths = set(list_of_strengths)

strength = pd.DataFrame(data=set_of_strengths, columns=['strength'])

strength['strength_id'] = strength.index

strength = strength[['strength_id', 'strength']]

# Creating the strength_junction table

strength_junction = cleaned_parent_df.copy()

strength_junction = strength_junction[['name_id', 'strengths']]

strength_junction.dropna(inplace=True, how='any')


def create_strength_junction_table(df):
    series_of_name_ids = []
    series_of_strengths = []

    for index, row in df.iterrows():
        list_of_strengths_in_row = row['strengths'].strip(
            '[').strip(']').replace("'", "").split(", ")
        for strength in list_of_strengths_in_row:
            series_of_name_ids.append(index)
            series_of_strengths.append(strength)

    return pd.DataFrame({'name_id': series_of_name_ids, 'strength': series_of_strengths})


strength_junction = create_strength_junction_table(strength_junction)

strength_to_strength_id_dict = dict(
    zip(strength['strength'], strength['strength_id']))

strength_junction['strength'] = strength_junction['strength'].map(
    strength_to_strength_id_dict)

strength_junction.rename(inplace=True, columns={'strength': 'strength_id'})

# Creating the weakness table

weakness = (cleaned_parent_df.copy())

weakness = weakness[['weaknesses']]

weakness.dropna(inplace=True)

weakness['weaknesses'] = weakness['weaknesses'].apply(lambda x: x.strip('['))
weakness['weaknesses'] = weakness['weaknesses'].apply(lambda x: x.strip(']'))

string_of_weaknesses = ", ".join(list(weakness['weaknesses'])).replace("'", "")

list_of_weaknesses = string_of_weaknesses.split(", ")

set_of_weaknesses = set(list_of_weaknesses)

weakness = pd.DataFrame(data=set_of_weaknesses, columns=['weakness'])

weakness['weakness_id'] = weakness.index

weakness = weakness[['weakness_id', 'weakness']]

# Creating the weakness_junction table

weakness_junction = cleaned_parent_df.copy()

weakness_junction = weakness_junction[['name_id', 'weaknesses']]

weakness_junction.dropna(inplace=True, how='any')


def create_weakness_junction_table(df):
    series_of_name_ids = []
    series_of_weaknesses = []

    for index, row in df.iterrows():
        list_of_weaknesses_in_row = row['weaknesses'].strip(
            '[').strip(']').replace("'", "").split(", ")
        for weakness in list_of_weaknesses_in_row:
            series_of_name_ids.append(index)
            series_of_weaknesses.append(weakness)

    return pd.DataFrame({'name_id': series_of_name_ids, 'weakness': series_of_weaknesses})


weakness_junction = create_weakness_junction_table(weakness_junction)

weakness_to_weakness_id_dict = dict(
    zip(weakness['weakness'], weakness['weakness_id']))

weakness_junction['weakness'] = weakness_junction['weakness'].map(
    weakness_to_weakness_id_dict)

weakness_junction.rename(inplace=True, columns={'weakness': 'weakness_id'})

# Creating tech_score table from erd, but calling it programming_language

programming_language_list = ['C#', 'Java', 'R',
                        'JavaScript', 'Python',
                        'C++', 'Ruby', 'SPSS', 'PHP']

programming_language_id_list = [0, 1, 2, 3, 4, 5, 6, 7, 8]

programming_language = pd.DataFrame({'programming_language_id': programming_language_id_list, 'programming_language': programming_language_list})

# Creating tech_junction table but calling tech_self_score

edited_df = cleaned_parent_df.copy()

edited_df = edited_df[['name_id', 'tech_self_score.C#']]

edited_df.dropna(inplace=True)

edited_df['programming_language'] = 'C#'

edited_df.rename(columns={'tech_self_score.C#': 'score'}, inplace=True)

def extract_name_id_score_and_language(df, language):

    edited_df = df.copy()

    edited_df = edited_df[['name_id', f'tech_self_score.{language}']]

    edited_df.dropna(inplace=True)

    edited_df['programming_language'] = language

    edited_df.rename(columns={f'tech_self_score.{language}': 'score'}, inplace=True)

    language_to_id_dict = dict(zip(programming_language['programming_language'], programming_language['programming_language_id']))

    edited_df['programming_language'] = edited_df['programming_language'].map(language_to_id_dict)

    edited_df.rename(inplace=True, columns={'programming_language': 'programming_language_id'})

    return edited_df

tech_self_score = pd.concat([
    extract_name_id_score_and_language(cleaned_parent_df, 'C#'),
    extract_name_id_score_and_language(cleaned_parent_df, 'Java'),
    extract_name_id_score_and_language(cleaned_parent_df, 'R'),
    extract_name_id_score_and_language(cleaned_parent_df, 'JavaScript'),
    extract_name_id_score_and_language(cleaned_parent_df, 'Python'),
    extract_name_id_score_and_language(cleaned_parent_df, 'C++'),
    extract_name_id_score_and_language(cleaned_parent_df, 'Ruby'),
    extract_name_id_score_and_language(cleaned_parent_df, 'SPSS'),
    extract_name_id_score_and_language(cleaned_parent_df, 'PHP')
    ])

tech_self_score = tech_self_score[['name_id', 'programming_language_id', 'score']]
