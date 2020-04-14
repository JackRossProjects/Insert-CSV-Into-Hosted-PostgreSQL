import pandas
import psycopg2
import os
from dotenv import load_dotenv
import json
import sqlite3
from psycopg2.extras import execute_values


def df_create(file_name):
    df = pandas.read_csv(file_name)
    # Replace apostrophes in name column to avoid SQL confusion
    df['Name'] = df['Name'].str.replace("'", '', regex=True)
    # Replace / and spaces in column names with underscores
    df.columns = df.columns.str.replace('/', "_")
    df.columns = df.columns.str.replace(' ', '_')
    return df

df = df_create('/home/jack/Desktop/titanic.csv')

# Instantiate connection to sqlite3
# Convert df to SQL
tconnection = sqlite3.connect('titanic.sqlite3')
df.to_sql('titanic', tconnection, if_exists='replace', index=False)
# Instantiate sqlite3 cursor
tcursor = tconnection.cursor()
# save SQL tabular data to result variable
result = tcursor.execute('SELECT * FROM titanic').fetchall()
# Instantiate postgresql connection & cursor
connection = psycopg2.connect(dbname='NA', user='NA', password='NA', host='drona.db.elephantsql.com')
cursor = connection.cursor()

print(result)

'''
#
# TABLE CREATION
#
query = """
CREATE TABLE IF NOT EXISTS titanic (
        Survived INTEGER,
        Pclass INTEGER,
        Name VARCHAR(500),
        Sex VARCHAR(7),
        Age INTEGER,
        Siblings_Spouses_Abroad INTEGER,
        Parents_children_Abroad INTEGER,
        Fare REAL
        )
"""
cursor.execute(query)

for row in result:
    insert_row = """
        INSERT INTO titanic
        (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Abroad, Parents_Children_Abroad, Fare)
        VALUES """ + str(row[:]) + ';'
    cursor.execute(insert_row)

cursor.close()
connection.commit()
'''
