import io
import logging
import os
from abc import ABC

import boto3
import pandas as pd
import requests
from botocore.config import Config


def load_config() -> dict:
    return {
        "data_bucket": os.getenv("DATA_BUCKET"),
        "s3_endpoint_url": os.getenv("S3_ENDPOINT_URL"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "feast_registry_destination": os.getenv("FEAST_REGISTRY_DESTINATION"),
        "feast_redis_host": os.getenv("FEAST_REDIS_HOST"),
        "feast_redis_port": os.getenv("FEAST_REDIS_PORT"),
        "feast_redis_password": os.getenv("FEAST_REDIS_PASSWORD"),
        "feast_s3_endpoint_url": os.getenv("FEAST_S3_ENDPOINT_URL"),
    }


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


class BasePipeline(ABC):
    def __init__(self, config: dict):
        self.config = config
        self.log = get_logger(self.__class__.__name__)

    def run(self):
        raise NotImplementedError("Subclasses must implement the run method")


class S3Client:
    def __init__(self, config: dict):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint_url"],
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            config=Config(s3={"addressing_style": "path"}),
        )
        self.log = get_logger(self.__class__.__name__)

    def save(self, content: bytes, bucket: str, object_key: str):
        self.s3.put_object(Bucket=bucket, Key=object_key, Body=content)

    def load_parquet(self, bucket: str, object_key: str) -> pd.DataFrame | None:
        try:
            response = self.s3.get_object(Bucket=bucket, Key=object_key)
            data = io.BytesIO(response["Body"].read())
            return pd.read_parquet(data, engine="fastparquet")
        except Exception as e:
            self.log.error(f"Failed to load parquet file: {e}")
            return None

    def load_csv(self, bucket: str, object_key: str) -> pd.DataFrame | None:
        try:
            response = self.s3.get_object(Bucket=bucket, Key=object_key)
            data = io.BytesIO(response["Body"].read())
            return pd.read_csv(data)
        except Exception as e:
            self.log.error(f"Failed to load CSV file: {e}")
            return None


class OpenMeteoClient:
    def __init__(self):
        self._archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self._forecast_url = "https://api.open-meteo.com/v1/forecast"
        self.log = get_logger(self.__class__.__name__)

    def fetch_historical_weather_data(self, params: dict) -> dict | None:
        response = requests.get(self._archive_url, params=params)

        if response.status_code != 200:
            self.log.error(
                f"Failed to fetch historical weather data: {response.status_code}"
            )
            return None

        return response.json()

    def fetch_forecast_weather_data(self, params: dict) -> dict | None:
        response = requests.get(self._forecast_url, params=params)

        if response.status_code != 200:
            self.log.error(
                f"Failed to fetch forecast weather data: {response.status_code}"
            )
            return None

        return response.json()
