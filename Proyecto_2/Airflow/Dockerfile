ARG AIRFLOW_VERSION=2.11.1
FROM apache/airflow:slim-${AIRFLOW_VERSION}

# Copy dependency files
COPY requirements.txt /

# Install dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY dags/ /opt/airflow/dags/