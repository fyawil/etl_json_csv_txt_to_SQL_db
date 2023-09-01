import sqlite3

def is_select_query(sql_query):
    # Remove leading and trailing whitespace
    sql_query = sql_query.strip()

    # Check if the query starts with "SELECT"
    if sql_query.startswith("SELECT"):
        # Check if the query contains any other SQL keywords that indicate updates
        if any(keyword in sql_query.upper() for keyword in ["INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"]):
            return False
        else:
            return True
    else:
        return False

# Creating the database
conn = sqlite3.connect('sparta.db')

while True:

    query = input('''
Please enter your query here or enter EXIT to exit: 
''')
    
    # Exiting the program
    if query.strip().upper() == 'EXIT':
        print('Goodbye!')
        break
    
    cursor = conn.cursor()

    if is_select_query(query) == False:
        print("Invalid query. Only SELECT queries are permitted. Please try again...")
        continue

    try:
        cursor.execute(query)
        for row in cursor:
            print(row)
    except:
        print(f'''Invalid SELECT query. Please try again...''')
        continue

conn.close()