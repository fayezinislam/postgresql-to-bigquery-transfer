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

import sys
import json
import os
from google.cloud import datacatalog_v1, bigquery
from google.cloud.exceptions import NotFound
from data_catalog_tagging import *
from data_transfer import *

project_id = os.getenv('PROJECT_ID')
location = os.getenv('LOCATION')
system = os.getenv('SYSTEM')
metadata_template_id = os.getenv('METADATA_TEMPLATE_ID')
tag_template_id = os.getenv('TAG_TEMPLATE_ID')
bq_location = os.getenv('BQ_LOCATION')

db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

api_prefix = os.getenv('API_PREFIX')
field_lookup = {}
field_lookup['integer'] = 'INT64'
field_lookup['bigint'] = 'INT64'
field_lookup['numeric'] = 'BIGDECIMAL'
field_lookup['decimal'] = 'BIGDECIMAL'
field_lookup['character_varying'] = 'STRING'
field_lookup['timestamp_without_time_zone'] = 'TIMESTAMP'
field_lookup['boolean'] = 'BOOLEAN'

datacatalog = datacatalog_v1.DataCatalogClient()

def get_by_resource(linked_resource_name):
    request = datacatalog_v1.LookupEntryRequest(
        linked_resource=linked_resource_name,
    )
    response = datacatalog.lookup_entry(request=request)
    return response

def get_by_name(name):
    resource = "/".join((api_prefix, name))
    request = datacatalog_v1.LookupEntryRequest(
        linked_resource=resource,
    )
    response = datacatalog.lookup_entry(request=request)
    return response
    
def get_fields(table):
    fields = []
    for col in table.schema.columns:
        fields.append(col)
    return fields

def get_field_names(table):
    fields = []
    for col in table.schema.columns:
        fields.append(col.column)
    return fields
    
def get_additive_fields(source_fields, dest_fields):
    new_fields = []
    for field in source_fields:
        if field not in dest_fields:
            new_fields.append(field)
    return new_fields

def postgres_field_to_bq(postgres_type):
    return field_lookup[postgres_type]

def generate_bq_schema(pg_columns):
    bq_columns = []
    for col in pg_columns:
        bq_columns.append(bigquery.SchemaField(col.column, postgres_field_to_bq(col.type)))
    return bq_columns    

def create_bq_table(full_table_name, columns):
    client = bigquery.Client()
    name = full_table_name
    table = bigquery.Table(name, columns)
    result = client.create_table(table)
    return result

def create_partitioned_bq_table(
        full_table_name, 
        columns, 
        part_type, 
        part_field, 
        clust_fields):

    client = bigquery.Client()
    name = full_table_name
    table = bigquery.Table(name, columns)

    type = bigquery.TimePartitioningType.DAY
    if part_type == 'HOUR':
        type = bigquery.TimePartitioningType.HOUR
    elif part_type == 'DAY':
        type = bigquery.TimePartitioningType.DAY
    elif part_type == 'MONTH':
        type = bigquery.TimePartitioningType.MONTH
    elif part_type == 'YEAR':
        type = bigquery.TimePartitioningType.MONTH

    table.time_partitioning = bigquery.TimePartitioning(
        type_=type,
        field=part_field
    )

    table.clustering_fields = clust_fields
    result = client.create_table(table)
    return result

def update_bq_schema(full_table_name, target_schema):
    client = bigquery.Client()
    table = client.get_table(full_table_name)
    table.schema = target_schema
    result = client.update_table(table, ["schema"])
    return result

def dataset_exists(full_dataset_name):
    try:
        client = bigquery.Client() 
        dataset = client.get_dataset(full_dataset_name)
        return True
    except:
        return False
        
def create_dataset(full_dataset_name):
    try:
        client = bigquery.Client() 
        dataset = bigquery.Dataset(full_dataset_name)
        dataset.location = bq_location
        client.create_dataset(dataset)
    except:
        print('Could not create dataset ' + full_dataset_name)
        exit(1)

def list_source_tables():
    return get_entrygroup_tables(project_id, location, system)

def get_metadata(entry_name):
    return get_replication_metadata(project_id, location, entry_name, tag_template_id)

###################################################

def main():
    # Tag source tables with replication template
    tag_entry_group(project_id, location, tag_template_id, system, metadata_template_id)

    # Get postgresql table list
    source_tables = list_source_tables()

    # Sync schema
    for src_table in source_tables:

        #Process table
        print(f'processing table {src_table.display_name}')

        metadata = get_metadata(src_table.name)
        if metadata['sync_enabled'] is False:
            print('sync not enabled for ' + src_table.display_name)
            continue

        bq_table_name = ".".join([project_id, \
                                metadata['destination_dataset'], \
                                metadata['destination_table']])
        src_fields = get_field_names(src_table)
        dst_table = get_bigquery_table(
            project_id, 
            metadata['destination_dataset'], 
            metadata['destination_table'])

        if dst_table is None:
            print('need to create ' + bq_table_name)
            if dataset_exists(project_id + '.' + metadata['destination_dataset']) is False:
                create_dataset(project_id + '.' + metadata['destination_dataset'])
            new_schema = generate_bq_schema(get_fields(src_table))

            if 'destination_partition_column' in metadata:
                part_type = metadata['destination_partition_type']
                part_col = metadata['destination_partition_column']
                clust_cols = metadata['destination_clustering_columns'].split(',')
                table = create_partitioned_bq_table(bq_table_name, new_schema, part_type, part_col, clust_cols)
                print('created partitioned table ' + bq_table_name)
            
            else:
                table = create_bq_table(bq_table_name, new_schema)
                print('created table ' + bq_table_name)                
            
        else:
            dst_fields = get_field_names(dst_table)
            new_fields = get_additive_fields(src_fields, dst_fields)
            if len(new_fields) == 0:
                print(f'no schema changes detected for {src_table.display_name}')
                
            else:
                new_schema = generate_bq_schema(get_fields(src_table))
                updated = update_bq_schema(bq_table_name, new_schema)
                print(f'schema updated for {src_table.display_name}')
                
        # Read records data from source table
        sql = f"SELECT * FROM {metadata['source_table']}"
        print(f"reading records from source table: {sql}")
        df = read_psql_db(sql)

        # Write to bigquery
        bq_schema = generate_bq_schema(get_fields(src_table))
        df = cast_dataframe_columns(df, bq_schema)
        write_df_to_bigquery(bq_table_name, df, bq_schema, metadata['write_disposition'])
        print(f"load job completed for {bq_table_name}")

main()

