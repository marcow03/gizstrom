from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    "docker_operator_dag",
    start_date=datetime(2025, 1, 1),
    schedule="5 * * * *",
    catchup=False
) as dag:

    t = DockerOperator(
        task_id="docker_command_sleep",
        image="python:3.12-slim",
        container_name="task_command_sleep",
        api_version="auto",
        auto_remove="success",
        command="python3 /pipelines/test.py",
        docker_url="tcp://compute-instance:2376",
        tls_ca_cert="/certs/client/ca.pem",
        tls_client_cert="/certs/client/cert.pem",
        tls_client_key="/certs/client/key.pem",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/pipelines", target="/pipelines", type="bind")
        ]
    )