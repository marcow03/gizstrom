from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    "docker_operator_dag",
    start_date=datetime(2025, 1, 1),
    schedule="5 * * * *",
    catchup=False
) as dag:

    # For running on the compute instance with TLS, use the following configuration.
    # t = DockerOperator(
    #     task_id="docker_command",
    #     image="python:3.12-slim",
    #     api_version="auto",
    #     auto_remove="success",
    #     command="python3 test.py",
    #     docker_url="tcp://compute-instance:2376",
    #     tls_ca_cert="/certs/client/ca.pem",
    #     tls_client_cert="/certs/client/cert.pem",
    #     tls_client_key="/certs/client/key.pem",
    #     network_mode="bridge",
    #     mount_tmp_dir=False,
    #     environment={
    #         "DOCKER_TLS_CERTDIR": "${DOCKER_TLS_CERTDIR}",
    #     }
    # )

    t = DockerOperator(
        task_id="docker_command_sleep",
        image="ghcr.io/marcow03/gizstrom/pipelines:latest",
        api_version="auto",
        auto_remove="success",
        command="uv run test.py",
        network_mode="production",
        mount_tmp_dir=False,
        environment={
            "TEST": os.getenv("TEST", "default_value")
        }
    )