import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config
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
    object_key = f"uploads/power_generation/{datetime.now(timezone.utc).date().isoformat()}.csv"

    print(f"Got file: {file.filename}, content type: {file.content_type}")

    try:
        content = await file.read()
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=content,
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