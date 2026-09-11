FROM apache/airflow:2.10.5

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

COPY requirements.txt /tmp/requirements.txt

# Airflow's constraints keep a transitive upgrade from breaking the providers.
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
