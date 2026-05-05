import io
import os

import boto3
import pandas as pd
import requests
from botocore.config import Config
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.logger import logger
from fastapi.staticfiles import StaticFiles

BUCKET = os.getenv("DATA_BUCKET", "data")
INFERENCE_ENDPOINT_URL = os.getenv(
    "INFERENCE_ENDPOINT_URL", "http://inference-endpoint:8001/predict"
)
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow:8080")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": "path"}),
    )


def trigger_airflow_dag():
    token_resp = requests.post(
        f"{AIRFLOW_BASE_URL}/auth/token",
        # The airflow instance is not secured by a password.
        # The API requires a username and password, but they can be anything.
        data={"username": "admin", "password": "admin"},
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    token = token_resp.json().get("access_token")
    if not token:
        logger.error(f"Failed to get Airflow token: {token_resp.text}")

    task_resp = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v2/dags/feature_and_training/dagRuns",
        json={
            "logical_date": pd.Timestamp.now().tz_localize("UTC").isoformat(),
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    if task_resp.status_code != 200:
        logger.error(f"Failed to trigger Airflow DAG: {task_resp.text}")


app = FastAPI()
s3 = get_s3_client()


@app.post("/upload/")
async def upload_file(file: UploadFile):
    logger.info(f"Got file: {file.filename}, content type: {file.content_type}")

    try:
        df = pd.read_csv(file.file)
        if "Datum und Uhrzeit" not in df.columns or "Gesamtanlage" not in df.columns:
            raise HTTPException(status_code=400, detail="CSV format not valid.")

        s3.put_object(
            Bucket=BUCKET,
            Key="uploads/power_generation/power_generation.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
            ContentType="application/csv",
        )

    except Exception as exc:
        logger.error(f"Failed to upload file to S3: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {exc}"
        ) from exc

    try:
        trigger_airflow_dag()
    except Exception as exc:
        logger.error(f"Failed to trigger Airflow DAG: {exc}")

    return {
        "filename": file.filename,
        "bucket": BUCKET,
        "key": "uploads/power_generation/power_generation.csv",
    }


@app.get("/weather/historical/")
async def get_historical_weather():
    try:
        response = s3.get_object(Bucket=BUCKET, Key="source/weather_data.parquet")
        data = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(data)
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Failed to retrieve historical weather data: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve historical weather data: {exc}"
        ) from exc


@app.get("/weather/forecast/")
async def get_forecast_weather():
    try:
        response = s3.get_object(Bucket=BUCKET, Key="source/forecast_data.parquet")
        data = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(data)
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Failed to retrieve forecast weather data: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve forecast weather data: {exc}"
        ) from exc


@app.get("/power-generation/historical/")
async def get_historical_power_generation():
    try:
        response = s3.get_object(Bucket=BUCKET, Key="source/power_generation.parquet")
        data = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(data).drop(columns=["location"])
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Failed to retrieve historical power generation data: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve historical power generation data: {exc}",
        ) from exc


@app.get("/power-generation/forecast/")
async def get_forecast_power_generation():
    try:
        response = requests.get(INFERENCE_ENDPOINT_URL)
        response.raise_for_status()
        json = response.json()
        output = []
        for i, pred in enumerate(json["pred_power_generation_kwh"]):
            output.append(
                {
                    "time": json["features"]["event_timestamp"][i],
                    "pred_power_generation_kwh": pred,
                }
            )

        return output

    except Exception as exc:
        logger.error(f"Failed to retrieve forecast power generation data: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve forecast power generation data: {exc}",
        ) from exc


app.mount("/", StaticFiles(directory="static", html=True), name="static")
