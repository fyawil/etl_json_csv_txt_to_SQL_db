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
    'name_id': 'INTEGER PRIMARY KEY', 'course_id': 'INTEGER PRIMARY KEY'
}
academy_result_col_def = {
    'name_id': 'INTEGER PRIMARY KEY', 'analytic': 'INTEGER', 'independent': 'INTEGER', 'determined': 'INTEGER', 'professional': 'INTEGER', 'studious': 'INTEGER', 'imaginative': 'INTEGER', 'week_no': 'INTEGER PRIMARY KEY'
}
trainer_col_def = {
    'trainer_id': 'INTEGER PRIMARY KEY', 'trainer_name': 'TEXT'
}
course_col_def = {
    'course_id': 'INTEGER PRIMARY KEY', 'course_name': 'TEXT'
}
trainer_junction_col_def = {
    'course_id': 'INTEGER PRIMARY KEY', 'trainer_id': 'INTEGER PRIMARY KEY'
}
candidate_col_def = {
    'name': 'TEXT', 'name_id': 'INTEGER PRIMARY KEY', 'interview_date': 'DATE', 'invited_by': 'TEXT', 'gender': 'TEXT', 'date_of_birth': 'DATE', 'email': 'TEXT', 'phone_number': 'TEXT', 'uni_degree': 'TEXT', 'self_development': 'TEXT', 'interview_result': 'TEXT', 'geoflex': 'TEXT', 'financial_support_self': 'TEXT', 'course_interest': 'TEXT', 'sparta_day_date': 'DATE', 'psychometric_result': 'INTEGER', 'presentation_result': 'INTEGER', 'sparta_day_location': 'TEXT', 'address_id': 'INTEGER'
}
strength_junction_col_def = {
    'name_id': 'INTEGER PRIMARY KEY', 'strength_id': 'INTEGER PRIMARY KEY'
}
strength_col_def = {
    'strength': 'TEXT', 'strength_id': 'INTEGER PRIMARY KEY'
}
weakness_junction_col_def = {
    'name_id': 'INTEGER PRIMARY KEY', 'weakness_id': 'INTEGER PRIMARY KEY'
}
weakness_col_def = {
    'weakness': 'TEXT', 'weakness_id': 'INTEGER PRIMARY KEY'
}
tech_self_score_col_def = {
    'name_id': 'INTEGER PRIMARY KEY', 'score': 'INTEGER', 'programming_language_id': 'INTEGER PRIMARY KEY'
}
programming_language_col_def = {
    'programming_language_id': 'INTEGER PRIMARY KEY', 'programming_language': 'TEXT'
}
address_col_def = {
    'address': 'TEXT', 'city': 'TEXT', 'postcode': 'TEXT', 'address_id': 'INTEGER PRIMARY KEY'
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

# Dictionary of foreign keys
foreign_keys = {
    'academy_result': [
        'FOREIGN KEY (name_id) REFERENCES candidate(name_id)'
    ],
    'course': [
        'FOREIGN KEY (course_id) REFERENCES trainer_junction(course_id)'
    ],
    'trainer': [
        'FOREIGN KEY (trainer_id) REFERENCES trainer_junction(trainer_id)'
    ],
    'trainer_junction': [
        'FOREIGN KEY (course_id) REFERENCES academy(course_id)'
    ],
    'academy': [
        'FOREIGN KEY (name_id) REFERENCES candidate(name_id)'
    ],
    'strength_junction': [
        'FOREIGN KEY (name_id) REFERENCES candidate(name_id)'
    ],
    'strength': [
        'FOREIGN KEY (strength_id) REFERENCES strength_junction(strength_id)'
    ],
    'weakness_junction': [
        'FOREIGN KEY (name_id) REFERENCES candidate(name_id)'
    ],
    'weakness': [
        'FOREIGN KEY (weakness_id) REFERENCES weakness_junction(weakness_id)'
    ],
    'tech_self_score': [
        'FOREIGN KEY (name_id) REFERENCES candidate(name_id)'
    ],
    'programming_language': [
        'FOREIGN KEY (programming_language_id) REFERENCES tech_self_score(programming_language_id)'
    ],
    'address': [
        'FOREIGN KEY (address_id) REFERENCES candidate(address_id)'
    ]
}


# Creating the database
conn = sqlite3.connect('sparta.db')

# Creating the tables in the database with primary keys and foreign keys
for table_name, col_def in column_definitions.items():
    columns = ', '.join(f'{col} {data_type}' for col, data_type in col_def.items())

    primary_keys = []
    split_columns_list = columns.split(', ')
    for split_col in split_columns_list:
        if 'PRIMARY KEY' in split_col:
            primary_keys.append(split_col.split(' ')[0])

    primary_keys = ", ".join(primary_keys)

    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns.replace(" PRIMARY KEY", "")}'

    if table_name in foreign_keys:
        foreign_key_definitions = ', '.join(fk for fk in foreign_keys[table_name])
        create_table_query += f', {foreign_key_definitions}'

    create_table_query_with_pk = create_table_query + f', PRIMARY KEY ({primary_keys}));'

    print(create_table_query_with_pk)
    conn.execute(create_table_query_with_pk)
    conn.commit()

# Populating the tables in the database with the imported dataframes
for df in dataframes:
    df.to_sql(get_df_name(df),conn,if_exists='replace',index=False)
    conn.commit()

conn.close()