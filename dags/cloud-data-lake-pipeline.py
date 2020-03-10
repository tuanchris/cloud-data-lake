from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'Tuan Nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('cloud-data-lake-pipeline',
          start_date=datetime.now(),
          schedule_interval='@once',
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'Start_pipeline',
    dag = dag
)

load_us_cities_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_us_cities_demo',
    bucket = 'gs://cloud-data-lake-gcp',
    source_objects = '/cities/us-cities-demographics.csv',
    destination_project_dataset_table = 'cloud-data-lake:IMMIGRATION_DWH_STAGING.us_cities_demo',
    source_format = 'csv',
    autodetect = True,
    skip_leading_rows = 1
)


dag >> start_pipeline >> load_us_cities_demo
