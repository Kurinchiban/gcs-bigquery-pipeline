# GCS to BigQuery Data Pipeline

## Overview
This project sets up a data pipeline that reads a CSV file from Google Cloud Storage (GCS) and loads it into a BigQuery table using Apache Beam and Dataflow.

## Prerequisites
- A Google Cloud Platform (GCP) account
- Google Cloud SDK installed
- Python installed (version 3.7 or later)

## Setup Instructions

### Step 1: Create a GCP Account
Sign up for a Google Cloud Platform (GCP) account if you don’t already have one.

### Step 2: Create a Service Account and Assign Roles
1. Navigate to IAM & Admin in the GCP Console.
2. Create a new service account.
3. Assign the following roles:
   - BigQuery Admin
   - Compute Admin
   - Storage Admin
   - Dataflow Admin
   - Dataflow Worker
   - Storage Object Viewer
4. Download the service account key (JSON file).
5. Set up the environment variable for authentication:
   ```sh
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
   ```

### Step 3: Create a GCS Bucket
1. Navigate to Cloud Storage in the GCP Console.
2. Create a new bucket.
3. Upload the `orders.csv` file to the bucket.

### Step 4: Create a BigQuery Dataset and Table
1. In BigQuery, create a new dataset.
2. Create a table using the following SQL command:
   ```sql
   CREATE TABLE orders (
       order_id INTEGER PRIMARY KEY,
       product STRING NOT NULL,
       quantity INTEGER NOT NULL,
       order_status STRING NOT NULL,
       order_date DATE NOT NULL
   );
   ```

### Step 5: Install Required Dependencies
Run the following command to install necessary Python packages:
```sh
pip install -r requirements.txt
```

### Step 6: Run the Dataflow Pipeline
Execute the following command to run the pipeline:
```sh
python3 data_flow_pipeline.py \
    --input "gs://pipeline-bucket-003/orders.csv" \
    --output "my-first-pipeline-453507.orders.completed_orders" \
    --region us-central1
```

## Additional Notes
- Ensure that the service account has sufficient permissions to access GCS and BigQuery.
- Verify that the `orders.csv` file follows the expected schema before running the pipeline.
- Check logs in Cloud Dataflow to troubleshoot any errors.
