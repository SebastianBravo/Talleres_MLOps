import os
import mysql.connector
import pandas as pd
from palmerpenguins import load_penguins


def connect_to_mysql():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    print("Connected to MySQL database")
    return connection


def close_mysql_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    print(f"Query executed: {query[:80]}")


def delete_table_if_exists(connection, table_name):
    execute_query(connection, f"DROP TABLE IF EXISTS {table_name}")
    print(f"Table '{table_name}' dropped")


def create_table_raw(connection, table_name):
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
    print(f"Table '{table_name}' created")


def insert_raw_penguin_data(connection, table_name):
    penguin_df = load_penguins()
    penguin_df = penguin_df.replace({float("nan"): None})

    cursor = connection.cursor()
    query = f"""INSERT INTO {table_name}
        (species, island, bill_length_mm, bill_depth_mm,
         flipper_length_mm, body_mass_g, sex, year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    values = [tuple(row) for row in penguin_df.values]
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    print(f"Inserted {len(values)} rows into '{table_name}'")


def load_data_from_mysql(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    df = pd.DataFrame(result, columns=columns)
    df.set_index("id", inplace=True)
    return df


def create_table_cleaned(connection, table_name):
    query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        species INT,
        bill_length_mm FLOAT,
        bill_depth_mm FLOAT,
        flipper_length_mm FLOAT,
        body_mass_g FLOAT,
        year INT,
        island_Biscoe BOOLEAN,
        island_Dream BOOLEAN,
        island_Torgersen BOOLEAN,
        sex_female BOOLEAN,
        sex_male BOOLEAN
    )
    """
    execute_query(connection, query)
    print(f"Table '{table_name}' created")


def preprocess_and_insert(connection, raw_table, cleaned_table):
    df = load_data_from_mysql(connection, raw_table)
    print(f"Datos raw: {len(df)} filas")

    rows_before = len(df)
    df = df.dropna()
    df = df.drop_duplicates()
    print(f"Filas eliminadas: {rows_before - len(df)}")

    species_map = {"Adelie": 0, "Chinstrap": 1, "Gentoo": 2}
    df["species"] = df["species"].map(species_map)

    df = pd.get_dummies(df, columns=["island", "sex"], dtype=int)

    print(f"Datos limpios: {len(df)} filas")
    print(f"Columnas: {list(df.columns)}")
    print(df.head())

    create_table_cleaned(connection, cleaned_table)

    columns = "species, bill_length_mm, bill_depth_mm, flipper_length_mm, " \
              "body_mass_g, year, island_Biscoe, island_Dream, island_Torgersen, " \
              "sex_female, sex_male"
    placeholders = ", ".join(["%s"] * 11)
    query = f"INSERT INTO {cleaned_table} ({columns}) VALUES ({placeholders})"

    col_order = [
        "species", "bill_length_mm", "bill_depth_mm", "flipper_length_mm",
        "body_mass_g", "year", "island_Biscoe", "island_Dream",
        "island_Torgersen", "sex_female", "sex_male",
    ]
    values = [tuple(row) for row in df[col_order].values]

    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    print(f"Inserted {len(values)} rows into '{cleaned_table}'")
