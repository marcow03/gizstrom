import io
import os

import boto3
import pandas as pd
from botocore.config import Config
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.logger import logger
from fastapi.staticfiles import StaticFiles

app = FastAPI()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": "path"}),
    )


@app.post("/upload/")
async def upload_file(file: UploadFile):
    bucket = os.getenv("DATA_BUCKET", "data")
    object_key = "uploads/power_generation/power_generation.parquet"

    logger.info(f"Got file: {file.filename}, content type: {file.content_type}")

    try:
        df = pd.read_csv(file.file)
        if "Datum und Uhrzeit" not in df.columns or "Gesamtanlage" not in df.columns:
            raise HTTPException(status_code=400, detail="CSV format not valid.")

        # drop second row (contains units)
        df = df.drop(index=0).reset_index(drop=True)
        df = df.rename(
            columns={
                "Datum und Uhrzeit": "date",
                "Gesamtanlage": "power_generation_kwh",
            }
        )
        df = df[["date", "power_generation_kwh"]]
        df["date"] = (
            pd.to_datetime(df["date"], dayfirst=True, errors="raise")
            .dt.tz_localize("Europe/Zurich")
            .dt.tz_convert("UTC")
        )
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
    except Exception as exc:
        logger.error(f"Failed to upload file to S3: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {exc}"
        ) from exc

    return {
        "filename": file.filename,
        "bucket": bucket,
        "key": object_key,
    }


app.mount("/", StaticFiles(directory="static", html=True), name="static")
