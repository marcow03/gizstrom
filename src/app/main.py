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


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": "path"}),
    )


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


@app.get("/power-generation/forecast/")
async def get_forecast_power_generation():
    try:
        response = requests.get(INFERENCE_ENDPOINT_URL)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logger.error(f"Failed to retrieve forecast power generation data: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve forecast power generation data: {exc}",
        ) from exc


app.mount("/", StaticFiles(directory="static", html=True), name="static")
