CREATE OR REPLACE TABLE `IMMIGRATION_DWH.D_AIRPORT` AS
SELECT
  substr(iso_region, 4,2) STATE_ID,
  name AIRPORT_NAME,
  iata_code IATA_CODE,
  local_code LOCAL_CODE,
  COORDINATES,
  ELEVATION_FT
FROM
  `cloud-data-lake.IMMIGRATION_DWH_STAGING.airport_codes`
where iso_country = 'US'
and type != 'closed'
