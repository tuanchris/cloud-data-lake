from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'cloud-data-lake-eb1bdfced02b.json')

client = bigquery.Client('cloud-data-lake', credentials = credentials)

client.create_dataset('IMMIGRATION_DWH_STAGING', exists_ok=True)
staging_dataset = client.dataset('IMMIGRATION_DWH_STAGING')
table = client.get_table('cloud-data-lake.IMMIGRATION_DWH_STAGING.immigration_data')
client.schema_to_json(table.schema,'./immigration_data.json')
