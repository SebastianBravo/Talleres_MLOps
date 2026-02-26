import os
import mysql.connector
import pandas as pd
from palmerpenguins import load_penguins

# Function to connect to MySQL database
def connect_to_mysql():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )
    print("Connected to MySQL database")
    return connection

# Function to close MySQL connection
def close_mysql_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")

# Function to execute a query
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    print("Query executed successfully")
    return cursor.fetchall()

# Function to delet table if exists
def delete_table_if_exists(connection, table_name):
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Table {table_name} deleted if it existed")

# Function to create a table for penguins dataset
def create_table_raw(connection, table_name):
    # Create table with appropriate schema for penguins dataset allowing for missing values
    query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        species VARCHAR(255),
        island VARCHAR(255),
        bill_length_mm FLOAT,
        bill_depth_mm FLOAT,
        flipper_length_mm FLOAT,
        body_mass_g FLOAT,
        sex VARCHAR(255),
        year INT
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created successfully")

def create_table_cleaned(connection, table_name):
    # Create table with appropriate schema for cleaned penguins dataset
    query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created successfully")

# Function to insert raw penguin data into the table
def insert_raw_penguin_data(connection, table_name):
    penguin_df = load_penguins()
    cursor = connection.cursor()

    # Prepare the SQL query for inserting data
    query = f"INSERT INTO {table_name} (species, island, bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g, sex, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    
    # Replace NaN values with None for proper NULL handling in MySQL
    penguin_df = penguin_df.replace({float('nan'): None})
    
    # Convert DataFrame to a list of tuples for insertion
    val = [tuple(row) for row in penguin_df.values]

    # Execute the query for multiple rows
    cursor.executemany(query, val)
    
    # Commit the transaction
    connection.commit()
    print(f"Penguin data inserted into table {table_name}")

# Function to load penguin data from MySQL database
def load_data_from_mysql(connection, table_name):
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    # Load the result into a DataFrame with column id as index
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    df.set_index('id', inplace=True)
    
    return df
