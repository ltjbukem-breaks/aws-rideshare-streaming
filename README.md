# aws-rideshare-streaming
Project to demonstrate usage of AWS DE tools simulating a livestreaming analytic pipeline with rideshare data

## Folder Architecture
```text
aws-rideshare-streaming/
│
├── README.md                  # Project overview, architecture diagram, instructions
├── requirements.txt           # Python dependencies for Lambdas or local scripts
├── architecture-diagram.png   # Visual diagram of the pipeline
│
├── event_generator/
│   ├── lambda_function.py     # Optional: Lambda version of event generator
│   ├── event_generator.py     # Local Python script to simulate events
│   ├── config.json            # Configurable parameters: cities, event rates, batch size
│   └── utils.py               # Helper functions (e.g., random trip generator)
│
├── lambda_processor/
│   ├── lambda_function.py     # Lambda to read raw JSON → flatten → Parquet → S3
│   ├── requirements.txt       # Lambda-specific dependencies (pyarrow, boto3)
│   ├── config.json            # S3 bucket names, partition keys, Athena DB/table info
│   └── helpers.py             # Partition handling, Athena partition updater
│
├── glue/
│   ├── crawler_config.json    # Glue crawler configuration (S3 path, database)
│   └── create_crawler.py      # Optional script to create or update crawler
│
├── athena/
│   ├── create_tables.sql      # SQL to create Athena external tables
│   ├── queries/
│   │   ├── active_trips_per_city.sql
│   │   ├── total_revenue_per_day.sql
│   │   └── average_fare_per_city.sql
│   └── materialized_views.sql # Optional views for analytics
│
├── sns/
│   └── sns_subscriptions.md   # Instructions to subscribe to failure notifications
│
├── tests/
│   ├── test_event_generator.py  # Unit tests for event generation logic
│   └── test_lambda_processor.py # Unit tests for Lambda transformation
│
└── docs/
    ├── pipeline_workflow.md     # Step-by-step workflow
    └── free_tier_cost.md        # Notes about keeping costs minimal on Free Tier
```

```text
Step 1: Set up your AWS environment

Create an S3 bucket for your project (e.g., rideshare-lake) with two main folders: ✅
raw/ → stores incoming JSON events ✅
curated/ → stores processed Parquet files ✅
Optionally create SNS topic for failure notifications.
Set up IAM roles:
Lambda role with S3 read/write access ✅
Lambda role with SNS publish permissions
Athena/Glue permissions (for partition updates and crawlers)
Set up CloudWatch for metrics and logging.

Step 2: Build the Event Generator ✅ DONE

Decide whether this will be:
Or a Lambda function triggered by EventBridge (more realistic) ✅
Implement event generation: ✅
Include trip_start and trip_end events ✅
Assign fares only for trip_end ✅
Include schema_version field ✅
Optionally simulate trip durations and bursts ✅
Write events to S3 raw folder: ✅
Optionally partition by year/month/day for raw files (optional) ✅
Keep batch sizes small to minimize Lambda invocations if using Lambda generator ✅

Step 3: Create Lambda Processor

Configure Lambda to trigger on S3 PUT events in the raw folder.
Responsibilities of Lambda:
Read JSON events ✅
Validate schema / flatten nested structures ✅
Convert JSON → Parquet ✅
Partition by city + year/month/day in curated folder ✅
Write Parquet to S3 ✅
Dynamically update Athena partitions (ALTER TABLE ADD PARTITION) ✅
Error handling:
If Lambda fails, publish a message to SNS
Optionally push failed events to SQS DLQ for retries
Logging & metrics:
Track events processed, failures, processing time
Push metrics to CloudWatch

Step 4: Configure Athena

Create an external table pointing to curated S3 folder.
Partitioned by year/month/day, optionally city.
Use Lambda to update partitions dynamically as new Parquet files arrive.
Optionally create materialized views for common analytics:
Active trips per city
Total revenue per day
Average fare by city
(Optional) Scheduled MSCK REPAIR TABLE or Glue crawler can help detect new partitions automatically.

Step 5: Set up Glue Crawler (daily) ✅ DONE

Schedule daily Glue crawler for curated folder:
Updates table schema in Athena
Detects new columns if schema changes in future
Configure crawler IAM permissions for S3 access.

Step 6: Monitoring & Notifications

Configure CloudWatch dashboards for:
Number of events processed per day
Lambda duration & errors
S3 bucket growth
Set CloudWatch alarms:
Trigger SNS if error rate > threshold
SNS notifications:
Subscribe your email (or Slack via webhook) to receive Lambda failures

Step 7: Optional Enhancements

Simulate realistic event timing:
Trip start → trip end after random duration
Variable event rates by city / time
Schema versioning for future field changes
Partitioning refinements:
City + year/month/day
QuickSight dashboards (Free Tier) to visualize revenue and trips
Document the pipeline in your GitHub repo:
Architecture diagram
Sample event
Example Athena queries
Free Tier cost considerations

✅ Step 8: Test End-to-End

Generate events locally or via Lambda
Confirm Lambda processor correctly converts to Parquet
Verify Athena partitions are updated dynamically
Check that failures trigger SNS notifications
Run sample queries to validate analytics
```