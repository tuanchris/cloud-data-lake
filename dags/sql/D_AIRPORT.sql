CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_AIRPORT` AS
SELECT DISTINCT
  substr(iso_region, 4,2) STATE_ID,
  name AIRPORT_NAME,
  iata_code IATA_CODE,
  local_code LOCAL_CODE,
  COORDINATES,
  ELEVATION_FT
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.airport_codes`
where iso_country = 'US'
and type != 'closed'
