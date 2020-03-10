import staging_schemas as ss
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import exceptions
import argparse
import logging
logging.basicConfig(level=logging.INFO)

def get_client(project_id, service_account):
    '''
    Get client object based on the project_id.
    Parameters:
        - project_id (str): id of project
        - service_account (str): path to a JSON service account. If the path
        is blanked, use application default authentication instead.
    '''
    if service_account:
        logging.info(f'Getting client from json file path {service_account}')
        credentials = service_account.Credentials.from_service_account_file(
            service_account)
        client = bigquery.Client(project_id, credentials = credentials)
    else:
        logging.info('Getting client from application default authentication')
        client = bigquery.Client(project_id)
    return client

def create_dataset(dataset_id, client):
    '''
    Create dataset in a project
    Parameteres:
        - dataset_id (str): ID of dataset to be created
        - client (obj): client object
    '''

    try:
        dataset = client.get_dataset(dataset_id)
    except exceptions.NotFound:
        logging.info(f'Creating dataset {dataset_id}')
        client.create_dataset(dataset_id)
    else:
        logging.info(f'Dataset not created. {dataset_id} already exists.')

def create_table(table_id, schema, dataset_id, client):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    try:
        table = client.create_table(table)
    except exceptions.Conflict:
        logging.info(f'Table not created. {table_id} already exists')
    else:
        logging.info(f'Created table {dataset_id}.{table_id}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Setup data-lake infrastructures')
    parser.add_argument('--project_id', default = 'cloud-data-lake')
    parser.add_argument('--service_account', default = '')
    args = parser.parse_args()

    # Get client
    client = get_client(args.project_id, args.service_account)
    # Create datasets
    create_dataset('IMMIGRATION_DWH_STAGING', client)
    create_dataset('IMMIGRATION_DWH', client)

    # Create staging tables
    for table_id in ss.tables:
        schema = eval('ss.'+table_id)
        create_table(table_id, schema, 'IMMIGRATION_DWH_STAGING', client)
