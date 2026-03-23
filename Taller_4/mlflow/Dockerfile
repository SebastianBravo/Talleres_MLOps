FROM python:3.9-slim

ENV MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
    AWS_ACCESS_KEY_ID=admin \
    AWS_SECRET_ACCESS_KEY=supersecret

RUN pip install --no-cache-dir mlflow boto3 psycopg2-binary

WORKDIR /app
EXPOSE 5000

CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--backend-store-uri", "postgresql+psycopg2://mlflow:mlflow@mlflow_db:5432/mlflow", \
     "--default-artifact-root", "s3://mlflows3/artifacts", \
     "--serve-artifacts"]
