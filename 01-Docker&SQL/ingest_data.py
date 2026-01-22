#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

def ingest_data(engine, url, target_table, file_type = 'csv', dtype = None):
    if file_type == 'csv':
        df_iter = pd.read_csv(
            url, 
            dtype = dtype,
            iterator = True, 
            chunksize = 100000
            )

    elif file_type == 'parquet':
        df = pd.read_parquet(url)
        df_iter = [df]

    total_rows = 0
    first_chunk = True

    for df_chunk in tqdm(df_iter):

        chunk_len = len(df_chunk)
        total_rows += chunk_len
        
        if first_chunk:
            df_chunk.head(0).to_sql(
                name = target_table, 
                con = engine, 
                if_exists = 'replace'
            )
            first_chunk = False

        df_chunk.to_sql(
            name = target_table, 
            con = engine, 
            if_exists = 'append'
        )
    
    print(f"Ingest {total_rows} rows data into {target_table}")

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    trip_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
    zone_data_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    zone_data_dtype = {
        "LoactionID": "Int64",
        "Borough": "string",
        "Zone": "string",
        "service_zone": "string"
    }
    ingest_data(engine, trip_data_url, "trip_data", "parquet")
    ingest_data(engine, zone_data_url, "zone", "csv", zone_data_dtype)

if __name__ == '__main__':
    main()
