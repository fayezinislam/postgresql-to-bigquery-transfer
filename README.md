# postgresql-to-bigquery-transfer
The postgresql-to-bigquery-transfer adapter provides an easy way to sync postgresql database schema and transfer data to BigQuery tables in Google Cloud. 

## Before you begin
- [Select or create a Cloud Platform project](https://console.cloud.google.com/project?_ga=2.220968858.3275545.1654003980-1401993212.1652797137).
- [Enable billing for your project](https://support.google.com/cloud/answer/6293499#enable-billing).
- [Enable the Google Cloud Data Catalog API](https://console.cloud.google.com/flows/enableapi?apiid=datacatalog.googleapis.com).
- [Enable the Google Cloud BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery&_ga=2.78517780.1418766229.1655832390-504257537.1655302631).
- [Set up authentication with a service account so you can access the API from your local workstation](https://cloud.google.com/docs/authentication/getting-started).
- [Install and setup Data Catalog Connector for PostgreSQL](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-postgresql-connector).

## Installation
Install this library in a [virtualenv](https://virtualenv.pypa.io/en/latest/) using pip. virtualenv is a tool to create isolated Python environments. The basic problem it addresses is one of dependencies and versions, and indirectly permissions.

With virtualenv, it’s possible to install this library without needing system install permissions, and without clashing with the installed system dependencies.

### Mac/Linux
```
pip install virtualenv
virtualenv <your-env>
source <your-env>/bin/activate
```

### Windows
```
pip install virtualenv
virtualenv <your-env>
<your-env>\Scripts\activate
```

## Required libraries
```
pip install google-cloud
pip install google-cloud-bigquery
pip install google-cloud-datacatalog
pip install numpy
pip install pandas
pip install pg8000
pip install protobuf
pip install psycopg2
pip install SQLAlchemy
```

## Environment setup

### Auth credentials

#### Create a service account and grant it below roles

- Project Owner

#### Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/credentials.json`

> Please notice this folder and file will be required in next steps.

### Set environment variables

Replace below values according to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=credentials_file

export PROJECT_ID=google_cloud_project_id
export LOCATION='us-east4'
export SYSTEM='postgresql'
export METADATA_TEMPLATE_ID='postgresql_table_metadata'
export TAG_TEMPLATE_ID='data_replication_tags'
export BQ_LOCATION='us-east4'
export API_PREFIX='//datacatalog.googleapis.com'
export BQ_TABLE_CONFIG='table_config.json'

export DB_USER=postgresql_username
export DB_PASS=postgresql_password
export DB_HOST=postgresql_host
export DB_PORT=postgresql_port
export DB_NAME=postgresql_database
```

## Run application
```
python3 main.py
```
