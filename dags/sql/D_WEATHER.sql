CREATE OR REPLACE TABLE `IMMIGRATION_DWH.D_WEATHER` AS
SELECT DISTINCT
  EXTRACT(year
  FROM
    dt) YEAR,
  CITY,
  COUNTRY,
  LATITUDE,
  LONGITUDE,
  AVG(AverageTemperature) AVERAGE_TEMPERATURE,
  AVG(AverageTemperatureUncertainty) AVERAGE_TEMPERATURE_UNCERTAINTY
FROM
  `cloud-data-lake.IMMIGRATION_DWH_STAGING.temperature_by_city`
WHERE
  COUNTRY = 'United States'
  AND EXTRACT(year
  FROM
    dt) = (
  SELECT
    MAX(EXTRACT(year
      FROM
        dt))
  FROM
    `cloud-data-lake.IMMIGRATION_DWH_STAGING.temperature_by_city`)
GROUP BY
  EXTRACT(year
  FROM
    dt),
  CITY,
  COUNTRY,
  LATITUDE,
  LONGITUDE
