import os
import requests
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split

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

# Function to get data from a FastAPI source (API_URL)
def get_data_from_api():
    url = f"{os.getenv('API_URL')}/data"
    params = {"group_number": os.getenv("API_GROUP_NUMBER")}

    # Make the GET request to the API
    try:
        print(f"Obteniendo datos de la API en {url} ...")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        print(f"Batch number: {data['batch_number']}")
        print(f"Group number: {data['group_number']}")
        print(f"Data from API: {len(data['data'])} records")
        return pd.DataFrame(data['data'])
    except requests.exceptions.RequestException as e:
        data = response.json()
        if data['detail'] == 'Ya se recolectó toda la información minima necesaria':
            print("Ya se recolectó toda la información minima necesaria. No se cargarán nuevos datos.")
        else:
            print(f"Error loading data from API: {e}")
        return None  # Return empty DataFrame on error
    
# Function to delete table if exists
def delete_table_if_exists(connection, table_name):
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Table {table_name} deleted if it existed")

# Function to create a table for covertype dataset
def create_table_raw(connection, table_name):
    # Create table with appropriate schema for covertype dataset allowing for missing values
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        elevation INT NULL,
        aspect INT NULL,
        slope INT NULL,
        horizontal_distance_to_hydrology INT NULL,
        vertical_distance_to_hydrology INT NULL,
        horizontal_distance_to_roadways INT NULL,
        hillshade_9am INT NULL,
        hillshade_noon INT NULL,
        hillshade_3pm INT NULL,
        horizontal_distance_to_fire_points INT NULL,
        wilderness_area VARCHAR(50) NULL,
        soil_type VARCHAR(50) NULL,
        cover_type INT NULL
    );
    """
    
    execute_query(connection, query)
    # If the table was created successfully, print a message
    print(f"Table {table_name} created successfully")

# Function to create cleaned table with appropriate schema for preprocessed covertype dataset
def create_table_cleaned(connection, table_name, feature_names):
    # Create table with appropriate schema for cleaned covertype dataset
    # Sanitize column names for MySQL (replace special chars with underscores)
    sanitized_cols = [name.replace("__", "_").replace(" ", "_") for name in feature_names]
    feature_columns = ", ".join([f"`{col}` FLOAT" for col in sanitized_cols])
    query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {feature_columns},
        dataset VARCHAR(10),
        cover_type INT
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created successfully with {len(feature_names)} features")

# Function to insert raw covertype data into the table
def insert_raw_covertype_data(connection, table_name, df=None):
    if df is None or df.empty:
        print("No se cargaron datos desde la API o el DataFrame está vacío. No se insertarán datos en MySQL.")
        return
    else:
        print(f"Cargando datos: {len(df)} filas, {len(df.columns)} columnas...")

        # Replace NaN values with None for proper NULL handling in MySQL
        df = df.replace({float('nan'): None})
        
        cursor = connection.cursor()

        # Prepare the SQL query for inserting data
        query = f"""INSERT INTO {table_name}
            (elevation, aspect, slope, horizontal_distance_to_hydrology,
            vertical_distance_to_hydrology, horizontal_distance_to_roadways, hillshade_9am, hillshade_noon, hillshade_3pm, horizontal_distance_to_fire_points,
            wilderness_area, soil_type, cover_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        # Convert DataFrame to a list of tuples for insertion
        val = [tuple(row) for row in df.values]

        # Execute the query for multiple rows
        cursor.executemany(query, val)
        
        # Commit the transaction
        connection.commit()
        print(f"Se cargaron datos en la tabla {table_name}")

# Function to load data from MySQL database
def load_data_from_mysql(connection, table_name):
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    # Load the result into a DataFrame with column id as index
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    df.set_index('id', inplace=True)
    
    return df

# Function to preprocess data and insert into cleaned table
def preprocess_and_insert(connection, raw_table, cleaned_table, preprocessor_path, test_size=0.2, random_state=42):
    # 1. Load raw data
    df = load_data_from_mysql(connection, raw_table)
    print(f"Datos raw: {len(df)} filas")

    # 2. Drop NaN and duplicates
    rows_before = len(df)
    df = df.dropna()
    df = df.drop_duplicates()
    print(f"Filas eliminadas: {rows_before - len(df)}")

    # 4. Separate X and y
    X = df.drop(columns=["cover_type"])
    y = df["cover_type"]

    # 5. Split into train and test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    print(f"Train: {len(X_train)}, Test: {len(X_test)}")

    # 6. Define numeric and categorical columns
    num_cols = ["elevation", "aspect", "slope", "horizontal_distance_to_hydrology", "vertical_distance_to_hydrology", "horizontal_distance_to_roadways", "hillshade_9am", "hillshade_noon", "hillshade_3pm", "horizontal_distance_to_fire_points"]
    cat_cols = ["wilderness_area", "soil_type"]

    # 7. Create preprocessing pipeline
    numeric_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    categorical_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, num_cols),
            ("cat", categorical_pipe, cat_cols)
        ],
        remainder="drop"
    )

    # 8. Fit preprocessor on train data only (avoid data leakage)
    preprocessor.fit(X_train)

    # 9. Transform all datasets
    X_train_p = preprocessor.transform(X_train)
    X_test_p = preprocessor.transform(X_test)

    print("Dimensión tras preprocessing:")
    print("X_train_p:", X_train_p.shape)
    print("X_test_p: ", X_test_p.shape)

    # # 10. Save preprocessor to models volume
    # os.makedirs(preprocessor_path, exist_ok=True)
    # preprocessor_path = os.path.join(preprocessor_path, "preprocessor.joblib")
    # joblib.dump(preprocessor, preprocessor_path)
    # print(f"Preprocessor guardado en: {preprocessor_path}")

    # 11. Create dataframes with processed data, preserving feature names
    n_features = X_train_p.shape[1]
    feature_cols = preprocessor.get_feature_names_out().tolist()
    print(f"Feature names: {feature_cols}")
    
    # Sanitize column names for MySQL compatibility
    sanitized_cols = [name.replace("__", "_").replace(" ", "_") for name in feature_cols]

    df_train = pd.DataFrame(X_train_p, columns=sanitized_cols)
    df_train["dataset"] = "train"
    df_train["cover_type"] = y_train.values

    df_test = pd.DataFrame(X_test_p, columns=sanitized_cols)
    df_test["dataset"] = "test"
    df_test["cover_type"] = y_test.values

    df_processed = pd.concat([df_train, df_test], ignore_index=True)

    print(f"Datos procesados: {len(df_processed)} filas, {n_features} features")
    print(df_processed.head())

    # 12. Create table and insert data
    create_table_cleaned(connection, cleaned_table, sanitized_cols)

    # 13. Insert data
    columns = ", ".join([f"`{col}`" for col in sanitized_cols] + ["dataset", "cover_type"])
    placeholders = ", ".join(["%s"] * (n_features + 2))
    query = f"INSERT INTO {cleaned_table} ({columns}) VALUES ({placeholders})"

    values = [tuple(row) for row in df_processed.values]

    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    print(f"Inserted {len(values)} rows into '{cleaned_table}'")