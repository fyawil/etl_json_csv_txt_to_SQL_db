# Importing sqlite3 and dataframes
import sqlite3
from clean_and_convert_raw_dfs_to_erd_dfs import academy, academy_result, trainer, course, trainer_junction, candidate, strength_junction, strength, weakness_junction, weakness, tech_self_score, programming_language, address

def get_df_name(df):
    '''Return the name of the dataframe'''
    name =[x for x in globals() if globals()[x] is df][0]
    return name

# List of cleaned dataframes
dataframes = [
    academy, academy_result, trainer, course, trainer_junction, candidate, strength_junction,
    strength, weakness_junction, weakness, tech_self_score, programming_language, address
]

# Dictionaries of column definitions with datatypes for the creation of the database
academy_col_def = {
    'name_id': 'INTEGER', 'course_id': 'INTEGER'
}
academy_result_col_def = {
    'name_id': 'INTEGER', 'analytic': 'INTEGER', 'independent': 'INTEGER', 'determined': 'INTEGER', 'professional': 'INTEGER', 'studious': 'INTEGER', 'imaginative': 'INTEGER', 'week_no': 'INTEGER'
}
trainer_col_def = {
    'trainer_id': 'INTEGER', 'trainer_name': 'TEXT'
}
course_col_def = {
    'course_id': 'INTEGER', 'course_name': 'TEXT'
}
trainer_junction_col_def = {
    'course_id': 'INTEGER', 'trainer_id': 'INTEGER'
}
candidate_col_def = {
    'name': 'TEXT', 'name_id': 'INTEGER', 'interview_date': 'DATE', 'invited_by': 'TEXT', 'gender': 'TEXT', 'date_of_birth': 'DATE', 'email': 'TEXT', 'phone_number': 'TEXT', 'uni_degree': 'TEXT', 'self_development': 'TEXT', 'interview_result': 'TEXT', 'geoflex': 'TEXT', 'financial_support_self': 'TEXT', 'course_interest': 'TEXT', 'sparta_day_date': 'DATE', 'psychometric_result': 'INTEGER', 'presentation_result': 'INTEGER', 'sparta_day_location': 'TEXT', 'address_id': 'INTEGER'
}
strength_junction_col_def = {
    'name_id': 'INTEGER', 'strength_id': 'INTEGER'
}
strength_col_def = {
    'strength': 'TEXT', 'strength_id': 'INTEGER'
}
weakness_junction_col_def = {
    'name_id': 'INTEGER', 'weakness_id': 'INTEGER'
}
weakness_col_def = {
    'weakness': 'TEXT', 'weakness_id': 'INTEGER'
}
tech_self_score_col_def = {
    'name_id': 'INTEGER', 'score': 'INTEGER', 'programming_language_id': 'INTEGER'
}
programming_language_col_def = {
    'programming_language_id': 'INTEGER', 'programming_language': 'TEXT'
}
address_col_def = {
    'address': 'TEXT', 'city': 'TEXT', 'postcode': 'TEXT', 'address_id': 'INTEGER'
}

# A parent dictionary holding the column of each table in the database
column_definitions = {
    'academy': academy_col_def,
    'academy_result': academy_result_col_def,
    'trainer': trainer_col_def,
    'course': course_col_def,
    'trainer_junction': trainer_junction_col_def,
    'candidate': candidate_col_def,
    'strength_junction': strength_junction_col_def,
    'strength': strength_col_def,
    'weakness_junction': weakness_junction_col_def,
    'weakness': weakness_col_def,
    'tech_self_score': tech_self_score_col_def,
    'programming_language': programming_language_col_def,
    'address': address_col_def
}

# Creating the database
conn = sqlite3.connect('sparta.db')

# Creating the tables in the database
for table_name, col_def in column_definitions.items():
    columns = ', '.join(f'{col} {data_type}' for col, data_type in col_def.items())
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'
    print(create_table_query)
    conn.execute(create_table_query)
    conn.commit()

# Populating the tables in the database with the imported dataframes
for df in dataframes:
    df.to_sql(get_df_name(df),conn,if_exists='replace',index=False)
    conn.commit()

conn.close()