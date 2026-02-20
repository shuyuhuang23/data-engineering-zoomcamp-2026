import os
import json
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from pathlib import Path
import requests

load_dotenv()

# Change this to your bucket name
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_and_upload(taxi_type):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020]:
        for month in range(1, 13):
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            # Download file
            print(f"Downloading {csv_gz_filename}...")
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            
            if response.status_code != 200:
                print(f"File {csv_gz_filename} not found, skipping.")
                continue

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Upload parquet to bigquery
            try:
                upload_to_bigquery(csv_gz_filepath, taxi_type)
            except Exception as e:
                print(f"Failed to upload {csv_gz_filename}: {e}")

            # Delete the downloaded file to save space
            csv_gz_filepath.unlink()

def upload_to_bigquery(csv_gz_path, taxi_type):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{taxi_type}_tripdata_new"
    print(f"Reading {csv_gz_path.name} with Pandas...")
    df = pd.read_csv(csv_gz_path, compression='gzip', low_memory=False)

    if taxi_type == 'yellow':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    else:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
        autodetect=True,
    )

    print(f"Uploading to BigQuery: {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Successfully loaded {csv_gz_path.name}")
    



if __name__ == "__main__":
    for taxi_type in ["yellow", "green"]:
        download_and_upload(taxi_type)