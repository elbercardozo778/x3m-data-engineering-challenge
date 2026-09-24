FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

# dbt vive en su propio venv: sus versiones chocan con las constraints de Airflow.
USER root
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && chown -R airflow:0 /opt/dbt-venv

USER airflow
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV PYTHONPATH=/opt/airflow/src \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt

COPY --chown=airflow:0 src /opt/airflow/src
COPY --chown=airflow:0 data_quality /opt/airflow/data_quality
COPY --chown=airflow:0 dbt /opt/airflow/dbt
# Los paquetes se instalan en el build para que las tareas no dependan de la red.
# Cosmos arma el DAG desde el manifest en vez de correr `dbt ls` en cada parseo.
RUN ${DBT_EXECUTABLE} deps --project-dir ${DBT_PROJECT_DIR} --profiles-dir ${DBT_PROFILES_DIR} \
    && ${DBT_EXECUTABLE} parse --project-dir ${DBT_PROJECT_DIR} --profiles-dir ${DBT_PROFILES_DIR}
COPY --chown=airflow:0 dags /opt/airflow/dags
