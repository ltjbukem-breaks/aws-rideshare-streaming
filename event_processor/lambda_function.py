import json
import boto3
import pandas as pd
import io
import re
from datetime import datetime
from urllib.parse import unquote_plus
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
athena = boto3.client('athena')

# Constants
ATHENA_OUTPUT_BUCKET = os.environ.get('ATHENA_OUTPUT_BUCKET', 'rideshare-lake')  # Use env var or default
ATHENA_FOLDER = "athena-results"
DATABASE = "rideshare_db"
TABLE = "curated"

# Validation constants
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
UUID_REGEX = re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
CURRENCY_REGEX = re.compile(r"^[A-Z]{3}$")


def validate_event(event: dict):
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in event:
            raise ValueError(f"Missing required field: {field}")
        if not isinstance(event[field], expected_type):
            raise TypeError(f"Field '{field}' must be {expected_type}, got {type(event[field])}")

    if event["event_type"] not in VALID_EVENT_TYPES:
        raise ValueError("event_type must be 'trip_start' or 'trip_end'")

    if not UUID_REGEX.match(event["trip_id"]):
        raise ValueError("trip_id must be a valid UUID")

    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        raise ValueError("timestamp must be ISO 8601 format")

    if not CURRENCY_REGEX.match(event["currency_code"]):
        raise ValueError("currency_code must be 3 uppercase letters")

    if event["event_type"] == "trip_start" and event["fare_amount"] != 0:
        raise ValueError("trip_start fare_amount must be 0")
    if event["event_type"] == "trip_end" and event["fare_amount"] <= 0:
        raise ValueError("trip_end fare_amount must be > 0")

    return True


def add_partition(bucket, key):
    """
    Add partition to Athena pointing to curated Parquet path
    """
    # Use curated folder for Athena partition
    curated_key_path = key.replace('raw/', 'curated/')
    s3_path = os.path.dirname(curated_key_path) + "/"

    # Extract year/month/day from path
    match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", key)
    if not match:
        raise ValueError("Partition structure not found in key")

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))

    query = f"""
    ALTER TABLE {DATABASE}.{TABLE} ADD IF NOT EXISTS
    PARTITION (year={year}, month={month:02d}, day={day:02d})
    LOCATION 's3://{bucket}/{s3_path}';
    """

    # Run Athena query with explicit output location
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{bucket}/{ATHENA_FOLDER}/'}  
    )

    logger.info(f"Athena partition added: s3://{bucket}/{s3_path}")
    return response


def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Read JSON from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        # Validate event
        validate_event(data)

        # Flatten JSON
        df = pd.json_normalize(data)

        # Convert to Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)

        # Curated key
        curated_key = key.replace('raw/', 'curated/').replace('.json', '.parquet')

        # Upload Parquet to S3
        s3.put_object(
            Bucket=bucket,
            Key=curated_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        logger.info(f"Processed {key} to {curated_key}")

        # Add Athena partition
        add_partition(bucket, key)

        return {'statusCode': 200, 'body': f'Processed {key} to {curated_key}'}

    except Exception as e:
        logger.error(f"Error processing {key if 'key' in locals() else 'unknown'}: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}