# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import required modules.
import json
import os
from google.cloud import datacatalog_v1
from google.api_core.exceptions import PermissionDenied
from google.cloud.datacatalog_v1.types import Tag

# Load json config from file
def load_json_file(filename):
    f = open(filename)
    data = json.load(f)
    f.close()
    return data

# Load table config
file = os.getenv('BQ_TABLE_CONFIG')
conf = load_json_file(file)

# Create data catalog client with default credentials
client = datacatalog_v1.DataCatalogClient()

# Creates a tag template for Data Replication
def create_tag_template(values):
    project_id = values.get("project_id")
    tag_template_id = values.get("tag_template_id")
    location = values.get("location")

    tag_template = datacatalog_v1.types.TagTemplate()
    tag_template.display_name = "Data Replication Tags"

    tag_template.fields["type"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["type"].display_name = "Data asset type"
    for display_name in ["SOURCE", "REPLICA"]:
        enum_value = datacatalog_v1.types.FieldType.EnumType.EnumValue(
            display_name=display_name
        )
        tag_template.fields["type"].type_.enum_type.allowed_values.append(
            enum_value
        )

    tag_template.fields["has_pii"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["has_pii"].display_name = "Has PII"
    tag_template.fields["has_pii"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.BOOL

    tag_template.fields["pii_columns"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["pii_columns"].display_name = "PII columns CSV"
    tag_template.fields["pii_columns"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING

    tag_template.fields["source_table"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["source_table"].display_name = "Source table"
    tag_template.fields["source_table"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING

    tag_template.fields["destination_table"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_table"].display_name = "Destination table"
    tag_template.fields["destination_table"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING    

    tag_template.fields["destination_dataset"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_dataset"].display_name = "Destination dataset"
    tag_template.fields["destination_dataset"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING  

    tag_template.fields["destination_partition_type"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_partition_type"].display_name = "Destination partition type"
    tag_template.fields["destination_partition_type"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING    

    tag_template.fields["destination_partition_column"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_partition_column"].display_name = "Destination partition column"
    tag_template.fields["destination_partition_column"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING    

    tag_template.fields["destination_clustering_columns"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_clustering_columns"].display_name = "Destination clustering columns"
    tag_template.fields["destination_clustering_columns"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING    


    tag_template.fields["last_synced"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["last_synced"].display_name = "Last synced timestamp"
    tag_template.fields["last_synced"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING    

    tag_template.fields["destination_field_data_type"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["destination_field_data_type"].display_name = "Destination field data type"
    tag_template.fields["destination_field_data_type"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.STRING

    tag_template.fields["sync_enabled"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["sync_enabled"].display_name = "Sync enabled"
    tag_template.fields["sync_enabled"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.BOOL

    tag_template.fields["write_disposition"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["write_disposition"].display_name = "Write disposition"
    for display_name in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
        enum_value = datacatalog_v1.types.FieldType.EnumType.EnumValue(
            display_name=display_name
        )
        tag_template.fields["write_disposition"].type_.enum_type.allowed_values.append(
            enum_value
        )

    tag_template.fields["backfill_new_column_data"] = datacatalog_v1.types.TagTemplateField()
    tag_template.fields["backfill_new_column_data"].display_name = "Backfill new column data"
    tag_template.fields["backfill_new_column_data"].type_.primitive_type = datacatalog_v1.types.FieldType.PrimitiveType.BOOL

    expected_template_name = datacatalog_v1.DataCatalogClient.tag_template_path(
        project_id, location, tag_template_id
    )

    try:
        tag_template = client.create_tag_template(
            parent=f"projects/{project_id}/locations/{location}",
            tag_template_id=tag_template_id,
            tag_template=tag_template,
        )
        print(f"Created template: {tag_template.name}")
        return tag_template   
    except Exception as e:
        print(f"Cannot create template: {expected_template_name}")
        print(f"{e}")
        return None


# Get tag template by name
def get_tag_template(values):
    project_id = values.get("project_id")
    tag_template_id = values.get("tag_template_id")
    location = values.get("location")
    template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template_id}"

    request = datacatalog_v1.GetTagTemplateRequest(
        name=template_name,
    )

    try:
        response = client.get_tag_template(request=request)
        return response
    except PermissionDenied as e:
        print(f"Cannot get template: {e.message}")
        return None


# Get or create tag template if it does not exist
def get_or_create_tag_template(values):
    response = get_tag_template(values)
    if response == None:
        return create_tag_template(values)
    else:
        return response

# Get entries from entry group
def get_entries(values):
    project_id = values.get("project_id")
    entry_group_id = values.get("entry_group_id")
    location = values.get("location")
    resource_name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}"

    request = datacatalog_v1.ListEntriesRequest(
        parent=resource_name,
    )

    try:
        response = client.list_entries(request=request)
        return response
    except Exception as e:
        print(f"Cannot get entries: {e.message}")
        return None

# Get entry from data catalog by entry name
def get_entry(entry_name):
    request = datacatalog_v1.GetEntryRequest(
        name=entry_name,
    )
    try:
        response = client.get_entry(request=request)
        return response
    except Exception as e:
        print(f"Cannot get entry: {e.message}")
        return None

# List tags for entry
def list_tags(entry_id):
    client = datacatalog_v1.DataCatalogClient()
    # Initialize request argument(s)
    request = datacatalog_v1.ListTagsRequest(
        parent=entry_id,
    )

    try:
        page_result = client.list_tags(request=request)
        tags = []
        for response in page_result:
            tags.append(response)
        return tags
    except Exception as e:
        print(f"Cannot get tags: {e.message}")
        return None


# Get table config from Json file
def get_table_config(source_table):
    for i in conf['table_config']:
        if i['table'] == source_table:
            return i


# Create table tags with default values
def create_table_tags(tag_values):
    tag = datacatalog_v1.Tag()
    tag.template = tag_values.get('tag_template')

    tag.fields['type'] = datacatalog_v1.types.TagField()
    tag.fields['type'].enum_value.display_name = 'SOURCE'

    tag.fields['sync_enabled'] = datacatalog_v1.types.TagField()
    tag.fields['sync_enabled'].bool_value = True

    tag.fields['write_disposition'] = datacatalog_v1.types.TagField()
    tag.fields['write_disposition'].enum_value.display_name = tag_values.get('write_disposition')

    tag.fields['backfill_new_column_data'] = datacatalog_v1.types.TagField()
    tag.fields['backfill_new_column_data'].bool_value = True

    tag.fields['source_table'] = datacatalog_v1.types.TagField()
    tag.fields['destination_dataset'] = datacatalog_v1.types.TagField()
    if (tag_values.get('schema_name') == None):
        tag.fields['source_table'].string_value = tag_values.get('table_name')
        tag.fields['destination_dataset'].string_value = f"{tag_values.get('entry_group_id')}_{tag_values.get('database_name')}"
    else:
        tag.fields['source_table'].string_value = ".".join([
            tag_values.get('schema_name'), 
            tag_values.get('table_name')])
        tag.fields['destination_dataset'].string_value = f"{tag_values.get('entry_group_id')}_{tag_values.get('database_name')}_{tag_values.get('schema_name')}"


    tag.fields['destination_table'] = datacatalog_v1.types.TagField()
    tag.fields['destination_table'].string_value = tag_values.get('table_name')

    tag.fields['destination_table'] = datacatalog_v1.types.TagField()
    tag.fields['destination_table'].string_value = tag_values.get('table_name')

    source_table = ".".join([
            tag_values.get('schema_name'), 
            tag_values.get('table_name')])

    if (get_table_config(source_table) != None):
        tag.fields['destination_partition_column'] = datacatalog_v1.types.TagField()
        tag.fields['destination_partition_column'].string_value = get_table_config(source_table)['partition_column']

        tag.fields['destination_clustering_columns'] = datacatalog_v1.types.TagField()
        tag.fields['destination_clustering_columns'].string_value = get_table_config(source_table)['clustering_columns']

        tag.fields['destination_partition_type'] = datacatalog_v1.types.TagField()
        tag.fields['destination_partition_type'].string_value = get_table_config(source_table)['partition_type']
        

    request = datacatalog_v1.CreateTagRequest(
        parent=tag_values.get('entry_id'),
        tag=tag,
    )

    try:    
        response = client.create_tag(request=request)
        print(f"Tagged table [{tag_values.get('table_name')}] with template [{tag_values.get('tag_template')}]")
        return response
    except Exception as e:
        print(f"Cannot create tags: {e}")
        return None


# Get postgresql table metadata
def get_table_metadata(values, tag):
    project_id = values.get('project_id')
    location = values.get('location')
    metadata_template_id = values.get('metadata_template_id')

    metadata = {}
    template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{metadata_template_id}"
    if tag.template == template_name:
        metadata['database_name'] = tag.fields['database_name'].string_value
        metadata['schema_name'] = tag.fields['schema_name'].string_value
        return metadata

# Get postgresql table metadata
def get_psql_metadata(project_id, location, metadata_template_id, tag):
    metadata = {}
    template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{metadata_template_id}"
    if tag.template == template_name:
        metadata['database_name'] = tag.fields['database_name'].string_value
        metadata['schema_name'] = tag.fields['schema_name'].string_value
        return metadata


# Tag tables in entry group
def tag_entry_group_tables(values):
    tag_template = get_or_create_tag_template(values)  
    entries = get_entries(values)

    for entry in entries:
        if entry.user_specified_type == "table":
            process_table_tags(entry, values, tag_template.name)
    return None


# Get sync mode based on timestamp column availability
def get_sync_mode(entry):
    mode = 'WRITE_TRUNCATE'
    timestamp_fields = [field.column for field in entry.schema.columns 
            if field.type.lower().startswith('time') or 
                field.type.lower().startswith('date') or 
                field.type.lower().startswith('interval'
            )
        ]
    if len(timestamp_fields) > 0:
        mode = 'WRITE_APPEND'
    return mode

# Process table entry for tagging
def process_table_tags(entry, values, tag_template_name):
    # Get table tags
    tags = list_tags(entry.name)

    # Check if table is tagged with replication template
    is_tagged = False
    metadata = {}

    for tag in tags:
        #print(f"Table[{entry.name}] is already tagged with template[{tag.template}]")

        # Get source database and schema name
        metadata = get_table_metadata(values, tag)
        
        # Check if table is tagged
        if tag_template_name == tag.template:
            is_tagged = True
    #print(f"is_tagged [{entry.name}] = {is_tagged}")

    if not len(metadata) > 0:
        print(f"Skipped tagging [{entry.display_name}] due to missing postgresql table metadata")
        return

    # Tag table
    if is_tagged == False:
        tag_values = {
            "entry_id" : entry.name,
            "table_name": entry.display_name,
            "entry_group_id" : values.get("entry_group_id"),
            "database_name" : metadata["database_name"],
            "schema_name" : metadata["schema_name"],
            "tag_template" : tag_template_name,
            "write_disposition" : 'WRITE_TRUNCATE'
        }
        create_table_tags(tag_values)

def get_bq_schema(entry_name):
    return None


# Delete tag template and related tags for clean up
def delete_tag_template(values):
    tag_template = get_tag_template(values)
    request = datacatalog_v1.DeleteTagTemplateRequest(
        name=tag_template.name,
        force=True,
    )
    client.delete_tag_template(request=request)


# Tag entry group tables
def tag_entry_group(
    project_id, 
    location, 
    tag_template_id, 
    entry_group_id, 
    metadata_template_id):

    values={
        "project_id": project_id, #ftx-streaming-demo
        "location": location, #us-east4
        "tag_template_id": tag_template_id, #ftx_data_replication_tags
        "entry_group_id": entry_group_id, #postgresql
        "metadata_template_id": metadata_template_id, #postgresql_table_metadata
    }
    tag_entry_group_tables(values)

# Get table schema
def get_table_schema(project_id, location, entry_group_id, entry_id):
    entry_name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}/entries/{entry_id}"
    entry = get_entry(entry_name)
    schema = {}
    if hasattr(entry, "schema"):
        if hasattr(entry.schema, "columns"):
            for field in entry.schema.columns:
                schema[field.column] = field.type_
            return schema
    return None

# Get table schema by entry name
def get_table_schema_by_entry_name(entry_name):
    entry = get_entry(entry_name)
    schema = {}
    if hasattr(entry, "schema"):
        if hasattr(entry.schema, "columns"):
            for field in entry.schema.columns:
                schema[field.column] = field.type_
            #print(schema)
            return schema
    return None

# Get destination table metadata
def get_replication_metadata(project_id, location, entry_name, tag_template_id):
    tag_template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template_id}"
    tags = list_tags(entry_name)
    for tag in tags:
        if tag.template == tag_template_name:
            metadata = {}
            for field in tag.fields:
                # Handle other attributes of TagField class to future proof logic
                if len(tag.fields[field].string_value)>0:
                    metadata[field] = tag.fields[field].string_value
                elif len(tag.fields[field].enum_value.display_name)>0:
                    metadata[field] = tag.fields[field].enum_value.display_name
                elif tag.fields[field].timestamp_value != None:
                    metadata[field] = tag.fields[field].timestamp_value
                elif tag.fields[field].bool_value in (True, False):
                    metadata[field] = tag.fields[field].bool_value
            return metadata


# Get all tables names in entry group
def get_entrygroup_tables(project_id, location, entry_group_id):
    values={
        "project_id": project_id,
        "location": location,
        "entry_group_id": entry_group_id,
    }
    entries = get_entries(values)
    tables = []
    for entry in entries:
        if entry.user_specified_type == "table":
            tables.append(entry)
    return tables

# Get bigquery table 
def get_bigquery_table(project_id, dataset_id, table_id):
    resource_name = (
        f"//bigquery.googleapis.com/projects/{project_id}"
        f"/datasets/{dataset_id}/tables/{table_id}")
    try:
        table_entry = client.lookup_entry(
        request={"linked_resource": resource_name}
        )
        return table_entry
    except Exception as e:
        print(f"Cannot get bigquery table: {e.message}")
        return None


#Run program
def main():
    project_id = "ftx-streaming-demo"
    location = "us-east4"
    entry_group_id = "postgresql"
    metadata_template_id = "postgresql_table_metadata"
    entry_id = "reporting_fills"
    tag_template_id = "data_replication_tags"
    bq_location = "us-east4"

    # Tag source tables
    tag_entry_group(project_id, location, tag_template_id, entry_group_id, metadata_template_id)

