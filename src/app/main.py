import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile
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
    object_key = "uploads/power_generation/power_generation.csv"

    print(f"Got file: {file.filename}, content type: {file.content_type}")

    try:
        df = pd.read_csv(file.file)
        if 'Datum und Uhrzeit' not in df.columns or 'Gesamtanlage' not in df.columns:
            raise HTTPException(status_code=400, detail="CSV format not valid.")

        # drop second row (contains units)
        df = df.drop(index=0).reset_index(drop=True)
        df = df.rename(columns={
            'Datum und Uhrzeit': 'date',
            'Gesamtanlage': 'power_generation_kwh'
        })
        df = df[['date', 'power_generation_kwh']]
        csv_buffer = df.to_csv(index=False).encode('utf-8')

        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=csv_buffer,
            ContentType=file.content_type or "text/csv",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upload file to S3: {exc}") from exc

    return {
        "filename": file.filename,
        "bucket": bucket,
        "key": object_key,
    }

app.mount("/", StaticFiles(directory="static", html=True), name="static")