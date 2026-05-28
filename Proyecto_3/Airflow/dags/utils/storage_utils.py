import os

import boto3


def connect_to_minio():
    """Crea y devuelve un cliente de MinIO usando variables de entorno."""
    # Leer credenciales y endpoint desde variables de entorno
    minio_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    # Confirmar conexion
    print("Conectado a MinIO")
    return minio_client
