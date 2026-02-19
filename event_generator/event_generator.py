import boto3
import json
import uuid
import random
from datetime import datetime, timedelta
import time
import os

# ----------------------
# Configuration
# ----------------------
S3_BUCKET = os.environ['S3_BUCKET_NAME']
RAW_PREFIX = "raw/"
CITIES = ["London", "New York", "Tokyo", "Paris", "Berlin"]
EVENTS_PER_INVOCATION = 5
SCHEMA_VERSION = 1
CITY_CONFIG = {
    "London": {
        "currency_code": "GBP",
        "currency_symbol": "£"
    },
    "New York": {
        "currency_code": "USD",
        "currency_symbol": "$"
    },
    "Tokyo": {
        "currency_code": "JPY",
        "currency_symbol": "¥"
    },
    "Paris": {
        "currency_code": "EUR",
        "currency_symbol": "€"
    },
    "Berlin": {
        "currency_code": "EUR",
        "currency_symbol": "€"
    }
}

# AWS S3 client
s3 = boto3.client("s3")

# ----------------------
# Helper Functions
# ----------------------
def generate_trip_pair():
    trip_id = str(uuid.uuid4())
    driver_id = f"d{random.randint(1,100)}"

    city = random.choice(list(CITY_CONFIG.keys()))
    currency_code = CITY_CONFIG[city]["currency_code"]
    currency_symbol = CITY_CONFIG[city]["currency_symbol"]

    start_time = datetime.utcnow()
    trip_duration_minutes = random.randint(5, 45)
    end_time = start_time + timedelta(minutes=trip_duration_minutes)

    fare = round(random.uniform(5, 50), 2)

    trip_start = {
        "event_type": "trip_start",
        "trip_id": trip_id,
        "driver_id": driver_id,
        "city": city,
        "timestamp": start_time.isoformat(),
        "fare_amount": 0.0,
        "currency_code": currency_code,
        "currency_symbol": currency_symbol
    }

    trip_end = {
        "event_type": "trip_end",
        "trip_id": trip_id,
        "driver_id": driver_id,
        "city": city,
        "timestamp": end_time.isoformat(),
        "fare_amount": fare,
        "currency_code": currency_code,
        "currency_symbol": currency_symbol
    }

    return trip_start, trip_end

def upload_event_to_s3(event):
    """Upload a single event JSON to S3 with partitioned path."""
    dt = datetime.fromisoformat(event["timestamp"])
    key = (
        f"{RAW_PREFIX}year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"{event['trip_id']}_{event['event_type']}.json"
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(event))
    print(f"Uploaded {event['trip_id']} to s3://{S3_BUCKET}/{key}")

# -----------------------------
# Lambda handler
# -----------------------------
def lambda_handler(event, context):
    try:
        num_trips = event.get("num_trips", EVENTS_PER_INVOCATION)

        for _ in range(num_trips):
            start, end = generate_trip_pair()
            upload_event_to_s3(start)
            upload_event_to_s3(end)
        return {
            "statusCode": 200,
            "body": f"{num_trips} events generated"
        }
    except Exception as e:
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}
