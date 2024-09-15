FROM apache/airflow:2.9.3-python3.12

USER root

RUN usermod -u 214 airflow
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/src"

USER airflow

COPY src/ .

COPY requirements.txt ./

RUN uv pip install --no-cache "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
