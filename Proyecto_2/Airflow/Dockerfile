FROM apache/airflow:slim-2.11.1

# Copy dependency files
COPY requirements.txt /

# Install dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt