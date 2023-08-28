import pandas as pd
import numpy as np
import sqlite3
# from sqlalchemy.types import Integer
# from sqlalchemy.types import String
# from sqlalchemy.types import Date

from clean_and_convert_raw_dfs_to_erd_dfs import academy, academy_result, trainer, course, trainer_junction, candidate, strength_junction, strength, weakness_junction, weakness, tech_self_score, programming_language, address


def get_df_name(df):
    name = [x for x in globals() if globals()[x] is df][0]
    return name


def create_db(db_name, dataframes):
    conn = sqlite3.connect(db_name)
    for i, df in enumerate(dataframes):
        df.to_sql(get_df_name(df), conn, if_exists='replace',
                  index=False, #dtype=column_definitions[i]
                  )


# academy_col_def = {
#     'name_id': Integer(), 'course_id': Integer()
# }
# academy_result_col_def = {
#     'name_id': Integer(), 'analytic': Integer(), 'independent': Integer(), 'determined': Integer(), 'professional': Integer(), 'studious': Integer(), 'imaginative': Integer(), 'week_no': Integer()
# }
# trainer_col_def = {
#     'trainer_id': Integer(), 'trainer_name': String()
# }

# course_col_def = {
#     'course_id': Integer(), 'course_name': String()
# }
# trainer_junction_col_def = {
#     'course_id': Integer(), 'trainer_id': Integer()
# }
# candidate_col_def = {
#     'name': String(), 'name_id': Integer(), 'interview_date': Date(), 'invited_by': String(), 'gender': String(), 'date_of_birth': Date(), 'email': Date(), 'phone_number': String(), 'uni_degree': String(), 'self_development': String(), 'interview_result': String(), 'geoflex': String(), 'financial_support_self': String(), 'course_interest': String(), 'sparta_day_date': Date(), 'psychometric_result': Integer(), 'presentation_result': Integer(), 'sparta_day_location': String(), 'address_id': Integer()
# }
# strength_junction_col_def = {
#     'name_id': Integer(), 'strength_id': Integer()
# }
# strength_col_def = {
#     'strength': String(), 'strength_id': Integer()

# }
# weakness_junction_col_def = {
#     'name_id': Integer(), 'weakness_id': Integer()

# }
# weakness_col_def = {
#     'weakness': String(), 'weakness_id': Integer()
# }
# tech_self_score_col_def = {
#     'name_id': Integer(), 'score': Integer(), 'programming_language_id': Integer()
# }
# programming_language_col_def = {
#     'programming_language_id': Integer(), 'programming_language': String()

# }
# address_col_def = {
#     'address': String(), 'city': String(), 'postcode': String(), 'address_id': Integer()
# }

# column_definitions = [
#     academy_col_def, academy_result_col_def, trainer_col_def, course_col_def, trainer_junction_col_def,
#     candidate_col_def, strength_junction_col_def, strength_col_def, weakness_junction_col_def,
#     weakness_col_def, tech_self_score_col_def, programming_language_col_def, address_col_def
# ]

dataframes = [
    academy, academy_result, trainer, course, trainer_junction, candidate, strength_junction,
    strength, weakness_junction, weakness, tech_self_score, programming_language, address
]


create_db('sparta', dataframes)


# Selecting queries

conn = sqlite3.connect('sparta')
cursor = conn.cursor()

query = '''SELECT * FROM course WHERE course_name = "Business"'''

cursor.execute(query)

result = cursor.fetchall()

for row in result:
    print(row)

conn.close()
