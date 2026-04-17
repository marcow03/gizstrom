from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    "data_collection",
    start_date=datetime(2025, 1, 1),
    schedule="10 * * * *",
    catchup=False,
) as dag:
    t = DockerOperator(
        task_id="data_collection_task",
        image="ghcr.io/marcow03/gizstrom/pipelines:latest",
        api_version="auto",
        auto_remove="success",
        command="uv run main.py --pipeline data-collection",
        network_mode="production",
        mount_tmp_dir=False,
        environment={
            "S3_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "DATA_BUCKET": os.getenv("DATA_BUCKET"),
            "FEAST_REDIS_HOST": os.getenv("FEAST_REDIS_HOST"),
            "FEAST_REDIS_PORT": os.getenv("FEAST_REDIS_PORT"),
            "FEAST_REDIS_PASSWORD": os.getenv("FEAST_REDIS_PASSWORD"),
            "FEAST_REGISTRY_DESTINATION": os.getenv("FEAST_REGISTRY_DESTINATION"),
            "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI"),
        },
    )
