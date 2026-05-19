FROM python:3.9-slim

RUN pip install --no-cache-dir mlflow boto3 psycopg2-binary

WORKDIR /app
EXPOSE 5000

CMD mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri "postgresql+psycopg2://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@${MLFLOW_DB_HOST}:${MLFLOW_DB_PORT}/${MLFLOW_DB_NAME}" \
    --default-artifact-root s3://mlflows3/artifacts \
    --serve-artifacts
