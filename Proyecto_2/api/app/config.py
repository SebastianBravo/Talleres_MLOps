import os

MLFLOW_TRACKING_URI   = os.environ.get("MLFLOW_TRACKING_URI",        "http://mlflow:5000")
REGISTERED_MODEL_NAME = os.environ.get("REGISTERED_MODEL_NAME",      "diabetic-readmission-model")
MODEL_ALIAS           = os.environ.get("MODEL_ALIAS",                 "production")

POSTGRES_HOST     = os.environ.get("POSTGRES_DATASET_HOST",     "postgres-dataset")
POSTGRES_PORT     = os.environ.get("POSTGRES_DATASET_PORT",     "5432")
POSTGRES_USER     = os.environ.get("POSTGRES_DATASET_USER",     "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_DATASET_PASSWORD", "airflow")
POSTGRES_DB       = os.environ.get("POSTGRES_DATASET_DATABASE", "diabetic_data")
