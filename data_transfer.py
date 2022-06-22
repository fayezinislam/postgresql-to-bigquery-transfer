#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import numpy as np
import psycopg2 as pg
import pandas as pd
import sqlalchemy
import pg8000
import os
import data_catalog_tagging as dc

db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

client = bigquery.Client()

# Function which reads from our DB and returns results in DF
def read_psql_db(sql):
    url = sqlalchemy.engine.url.URL.create(
            drivername="postgresql",
            username=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            database=db_name)
    engine = sqlalchemy.create_engine(url)
    df = pd.read_sql(sql, con=engine)
    return df

def write_df_to_bigquery(table_id, df, schema, write_disposition):
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_disposition
    job_config.schema = schema

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

# Cast dataframe columns types
def cast_dataframe_columns(df, schema):
    for field in schema:
        if field.field_type == 'STRING':
            df[field.name] = df[field.name].astype(str)
            df[field.name].replace('None', np.nan, inplace=True)
        elif field.field_type in ('INT64','INTEGER'):
            df[field.name] = df[field.name].astype('Int64')
        elif field.field_type == 'TIMESTAMP':
            df[field.name] = pd.to_datetime(df[field.name])
        elif field.field_type == 'BIGDECIMAL':
            df[field.name] = df[field.name].astype('Float64')
    return df
        
def main():
    sql = "SELECT * FROM reporting.example_accounts"
    df = read_psql_db(sql)
    print(df.head())

    project_id = "ftx-streaming-demo"
    location = "us-east4"
    entry_group_id = "postgresql"
    entry_id = "reporting_balances"

    entry_name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}/entries/{entry_id}"
    entry = dc.get_entry(entry_name)
    
    for field in entry.schema.columns:
        print(f">>>{field['column']} : {field['type_']}")

    table_id = 'ftx-streaming-demo.test_reporting_dataset.accounts'
    schema = [
        bigquery.SchemaField("account_id", "INT64"),
        bigquery.SchemaField("coin_id", "INT64"),
        bigquery.SchemaField("size", "STRING"),
        bigquery.SchemaField("locked_in_stakes", "STRING"),
        bigquery.SchemaField("locked_in_spot_margin_funding_offers", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("epoch", "INT64")]
    write_disposition = 'WRITE_TRUNCATE'
    cast_dataframe_columns(df, schema)
    write_df_to_bigquery(table_id, df, schema, write_disposition)

