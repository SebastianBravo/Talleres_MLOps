import os

import psycopg2


def connect_to_db():
    """Abre una conexion a PostgreSQL usando variables de entorno."""
    # Leer credenciales de la base desde variables de entorno
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_DATASET_HOST"),
        user=os.getenv("POSTGRES_DATASET_USER"),
        password=os.getenv("POSTGRES_DATASET_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATASET_DATABASE"),
        port=os.getenv("POSTGRES_DATASET_PORT"),
    )
    # Confirmar conexion
    print("Conectado a la base de datos PostgreSQL")
    return connection


def close_db_connection(connection):
    """Cierra la conexion abierta si existe."""
    # Evitar cierre de conexiones nulas o ya cerradas
    if connection and not connection.closed:
        connection.close()
        print("Conexión a PostgreSQL cerrada")


def execute_query(connection, query):
    """Ejecuta una consulta SQL simple y retorna resultados si aplica."""
    # Ejecutar consulta
    cursor = connection.cursor()
    cursor.execute(query)
    # Confirmar transaccion
    connection.commit()
    print("Consulta ejecutada exitosamente")
    # Retornar filas si es una consulta con resultados
    if cursor.description:
        return cursor.fetchall()
    return []
