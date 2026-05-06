import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    "feature_and_training",
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["gizstrom", "training", "feature"],
) as dag:
    feature_task = DockerOperator(
        task_id="feature_task",
        image="ghcr.io/marcow03/gizstrom/pipelines:latest",
        auto_remove="success",
        command="uv run main.py --pipeline feature",
        network_mode="production",
        mount_tmp_dir=False,
        environment={
            "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "DATA_BUCKET": os.getenv("DATA_BUCKET"),
            "FEAST_REGISTRY_DESTINATION": os.getenv("FEAST_REGISTRY_DESTINATION"),
        },
    )

    training_task = DockerOperator(
        task_id="training_task",
        image="ghcr.io/marcow03/gizstrom/pipelines:latest",
        auto_remove="success",
        command="uv run main.py --pipeline training",
        network_mode="production",
        mount_tmp_dir=False,
        environment={
            "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "DATA_BUCKET": os.getenv("DATA_BUCKET"),
            "FEAST_REGISTRY_DESTINATION": os.getenv("FEAST_REGISTRY_DESTINATION"),
            "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI"),
        },
    )

    feature_task >> training_task
