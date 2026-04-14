import os

import boto3
import pandas as pd
from botocore.config import Config
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.logger import logger
from fastapi.staticfiles import StaticFiles

BUCKET = os.getenv("DATA_BUCKET", "data")
OBJECT_KEY = "uploads/power_generation/power_generation.csv"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": "path"}),
    )


app = FastAPI()


@app.post("/upload/")
async def upload_file(file: UploadFile):
    logger.info(f"Got file: {file.filename}, content type: {file.content_type}")

    try:
        df = pd.read_csv(file.file)
        if "Datum und Uhrzeit" not in df.columns or "Gesamtanlage" not in df.columns:
            raise HTTPException(status_code=400, detail="CSV format not valid.")

        s3 = get_s3_client()
        s3.put_object(
            Bucket=BUCKET,
            Key=OBJECT_KEY,
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
        "key": OBJECT_KEY,
    }


app.mount("/", StaticFiles(directory="static", html=True), name="static")
