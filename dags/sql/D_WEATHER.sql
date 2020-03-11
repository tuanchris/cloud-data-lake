CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_WEATHER` AS
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
  `{{ params.project_id }}.{{ params.staging_dataset }}.temperature_by_city`
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
    `{{ params.project_id }}.{{ params.staging_dataset }}.temperature_by_city`)
GROUP BY
  EXTRACT(year
  FROM
    dt),
  CITY,
  COUNTRY,
  LATITUDE,
  LONGITUDE
