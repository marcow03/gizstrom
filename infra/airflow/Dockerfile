FROM apache/airflow:slim-3.2.1-python3.14

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install apache-airflow-providers-docker
