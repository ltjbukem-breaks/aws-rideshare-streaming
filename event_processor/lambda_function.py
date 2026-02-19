import json
import boto3
import pandas as pd
import io
import re
from datetime import datetime
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

REQUIRED_FIELDS = {
    "event_type": str,
    "trip_id": str,
    "driver_id": str,
    "city": str,
    "timestamp": str,
    "fare_amount": (int, float),
    "currency_code": str,
    "currency_symbol": str,
}
VALID_EVENT_TYPES = {"trip_start", "trip_end"}
UUID_REGEX = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"
)
CURRENCY_REGEX = re.compile(r"^[A-Z]{3}$")

def validate_event(event: dict):
    # 1️⃣ Check required fields exist
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

        if not isinstance(event[field], expected_type):
            raise TypeError(
                f"Field '{field}' must be {expected_type}, "
                f"got {type(event[field])}"
            )

    # 2️⃣ Validate event_type
    if event["event_type"] not in VALID_EVENT_TYPES:
        raise ValueError("event_type must be 'trip_start' or 'trip_end'")

    # 3️⃣ Validate UUID format
    if not UUID_REGEX.match(event["trip_id"]):
        raise ValueError("trip_id must be a valid UUID")

    # 4️⃣ Validate timestamp format (ISO 8601)
    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        raise ValueError("timestamp must be ISO 8601 format")

    # 5️⃣ Validate currency code format
    if not CURRENCY_REGEX.match(event["currency_code"]):
        raise ValueError("currency_code must be 3 uppercase letters")

    # 6️⃣ Logical business rules
    if event["event_type"] == "trip_start" and event["fare_amount"] != 0:
        raise ValueError("trip_start fare_amount must be 0")

    if event["event_type"] == "trip_end" and event["fare_amount"] <= 0:
        raise ValueError("trip_end fare_amount must be > 0")

    return True

def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Read raw JSON from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        # Validate event
        validate_event(data)

        # Flatten nested JSON
        df = pd.json_normalize(data)

        # Convert to Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)

        # Create curated path
        curated_key = key.replace('raw/', 'curated/').replace('.json', '.parquet')

        # Upload to S3
        s3.put_object(
            Bucket=bucket,
            Key=curated_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        return {'statusCode': 200, 'body': f'Processed {key} to {curated_key}'}
    
    except Exception as e:
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}