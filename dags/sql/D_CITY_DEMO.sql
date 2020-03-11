CREATE OR REPLACE TABLE
  `{{ params.dwh_dataset }}.D_CITY_DEMO` AS
SELECT DISTINCT
  CITY CITY_NAME,
  STATE STATE_NAME,
  STATE_CODE STATE_ID,
  MEDIAN_AGE,
  MALE_POPULATION,
  FEMALE_POPULATION,
  TOTAL_POPULATION,
  NUMBER_OF_VETERANS,
  FOREIGN_BORN,
  AVERAGE_HOUSEHOLD_SIZE,
  AVG(
  IF
    (RACE = 'White',
      COUNT,
      NULL)) WHITE_POPULATION,
  AVG(
  IF
    (RACE = 'Black or African-American',
      COUNT,
      NULL)) BLACK_POPULATION,
  AVG(
  IF
    (RACE = 'Asian',
      COUNT,
      NULL)) ASIAN_POPULATION,
  AVG(
  IF
    (RACE = 'Hispanic or Latino',
      COUNT,
      NULL)) LATINO_POPULATION,
  AVG(
  IF
    (RACE = 'American Indian and Alaska Native',
      COUNT,
      NULL)) NATIVE_POPULATION
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.us_cities_demo`
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10
