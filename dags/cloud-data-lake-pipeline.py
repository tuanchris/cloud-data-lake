# Import packages
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

# Define default arguments
default_args = {
    'owner': 'Tuan Nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Define dag variables
project_id = 'cloud-data-lake'
staging_dataset = 'IMMIGRATION_DWH_STAGING'
dwh_dataset = 'IMMIGRATION_DWH'
gs_bucket = 'cloud-data-lake-gcp'

# Define dag
dag = DAG('cloud-data-lake-pipeline',
          start_date=datetime.now(),
          schedule_interval='@once',
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

# Load data from GCS to BQ
load_us_cities_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_us_cities_demo',
    bucket = gs_bucket,
    source_objects = ['cities/us-cities-demographics.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.us_cities_demo',
    schema_object = 'cities/us_cities_demo.json',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    field_delimiter=';',
    skip_leading_rows = 1
)

load_airports = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_airports',
    bucket = gs_bucket,
    source_objects = ['airports/airport-codes_csv.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.airport_codes',
    schema_object = 'airports/airport_codes.json',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
)

load_weather = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_weather',
    bucket = gs_bucket,
    source_objects = ['weather/GlobalLandTemperaturesByCity.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.temperature_by_city',
    schema_object = 'weather/temperature_by_city.json',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
)

load_immigration_data = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_immigration_data',
    bucket = gs_bucket,
    source_objects = ['immigration_data/*.parquet'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.immigration_data',
    source_format = 'parquet',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    autodetect = True
)

# Check loaded data not null
check_us_cities_demo = BigQueryCheckOperator(
    task_id = 'check_us_cities_demo',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.us_cities_demo`'

)

check_airports = BigQueryCheckOperator(
    task_id = 'check_airports',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.airport_codes`'
)

check_weather = BigQueryCheckOperator(
    task_id = 'check_weather',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.temperature_by_city`'
)


check_immigration_data = BigQueryCheckOperator(
    task_id = 'check_immigration_data',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.immigration_data`'
)

loaded_data_to_staging = DummyOperator(
    task_id = 'loaded_data_to_staging'
)

# Load dimensions data from files directly to DWH table
load_country = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_country',
    bucket = gs_bucket,
    source_objects = ['master_data/I94CIT_I94RES.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_COUNTRY',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'COUNTRY_ID', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'COUNTRY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

load_port = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_port',
    bucket = gs_bucket,
    source_objects = ['master_data/I94PORT.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_PORT',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'PORT_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PORT_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

load_state = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_state',
    bucket = gs_bucket,
    source_objects = ['master_data/I94ADDR.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_STATE',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'STATE_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'STATE_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

# Transform, load, and check fact data
create_immigration_data = BigQueryOperator(
    task_id = 'create_immigration_data',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/F_IMMIGRATION_DATA.sql'
)

check_f_immigration_data = BigQueryCheckOperator(
    task_id = 'check_f_immigration_data',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) = count(distinct cicid) FROM `{project_id}.{dwh_dataset}.F_IMMIGRATION_DATA`'
)

# Create remaining dimensions data
create_d_time = BigQueryOperator(
    task_id = 'create_d_time',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_TIME.sql'
)

create_d_weather = BigQueryOperator(
    task_id = 'create_d_weather',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_WEATHER.sql'
)

create_d_airport = BigQueryOperator(
    task_id = 'create_d_airport',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_AIRPORT.sql'
)

create_d_city_demo = BigQueryOperator(
    task_id = 'create_d_city_demo',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_CITY_DEMO.sql'
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

# Define task dependencies
dag >> start_pipeline >> [load_us_cities_demo, load_airports, load_weather, load_immigration_data]

load_us_cities_demo >> check_us_cities_demo
load_airports >> check_airports
load_weather >> check_weather
load_immigration_data >> check_immigration_data


[check_us_cities_demo, check_airports, check_weather,check_immigration_data] >> loaded_data_to_staging

loaded_data_to_staging >> [load_country, load_port, load_state] >> create_immigration_data >> check_f_immigration_data

check_f_immigration_data >> [create_d_time, create_d_weather, create_d_airport, create_d_city_demo] >> finish_pipeline
