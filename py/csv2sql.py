# importing csv module
import csv
import pandas as pd
from datetime import date
import mysql.connector
import time

# Importing and setting configurations
from congigs import SQL_configs
configs = SQL_configs()

# csv file name
filename = "data.csv"

# initializing the titles list
fields = []
data = []

connection =  mysql.connector.connect(
        host=configs.host,
        user=configs.user,
        password=configs.passwd,
        database=configs.database,
        auth_plugin=configs.auth_plugin
    )
try:
    initial_query = """
    DROP TABLE stocks;
    """
    # Drop the table
    cursor = connection.cursor()
    cursor.execute(initial_query)
    cursor.close()
    print('Droped existing table stocks')
except:
    print('No existing table found')

initial_query = """
CREATE TABLE stocks(
StockDate Date,
IndexValue VARCHAR( 20 ),
Open DECIMAL( 10, 4 ),
High DECIMAL( 10, 4 ),
Low DECIMAL( 10, 4 ),
Close DECIMAL( 10, 4 ),
AdjClose DECIMAL( 10, 4 ),
CloseUSD DECIMAL( 10, 4 ),
PRIMARY KEY (StockDate, IndexValue)
);
"""
# Initialize the table
time.sleep(2)
cursor = connection.cursor()
cursor.execute(initial_query)
cursor.close()
connection.close()
print('Created table stocks')
time.sleep(2)

connection =  mysql.connector.connect(
        host=configs.host,
        user=configs.user,
        password=configs.passwd,
        database=configs.database,
        auth_plugin=configs.auth_plugin
    )
cursor = connection.cursor()
with open(filename, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    fields = next(csvreader)

    # extracting each data row one by one
    for row in csvreader:
        yyyy = int(row[1].split('-')[0])
        mm = int(row[1].split('-')[1])
        dd = int(row[1].split('-')[2])

        data_to_insert = (
            row[0],
            date(yyyy, mm, dd),
            round(float(row[2]), 0),
            round(float(row[3]), 0),
            round(float(row[4]), 0),
            round(float(row[5]), 0),
            round(float(row[6]), 0),
            # Skipping Volume attribute as it is too large
            round(float(row[8]), 0)
        )
        data.append(data_to_insert)

    # get total number of rows
    print("Total no. of record(s) detected : %d"%(csvreader.line_num))

insert_query = "INSERT INTO stocks (IndexValue, StockDate, Open, High, Low, Close, AdjClose, CloseUSD) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

# Insert data into the table
cursor.executemany(insert_query, data)

# Commit the changes to the database
connection.commit()

print(f"Inserted {cursor.rowcount} record(s) into database")

# Closing connections
cursor.close()
connection.close()
