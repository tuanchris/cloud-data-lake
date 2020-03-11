CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.F_IMMIGRATION_DATA` AS
SELECT DISTINCT
  cicid CICID,
  CAST(i94yr AS NUMERIC) YEAR,
  CAST(i94mon AS NUMERIC) MONTH,
  COUNTRY_NAME COUNTRY_OF_ORIGIN,
  PORT_NAME,
  DATE_ADD('1960-1-1', INTERVAL CAST(arrdate AS INT64) DAY) ARRIVAL_DATE,
  CASE
    WHEN i94mode = 1 THEN 'Air'
    WHEN i94mode = 2 THEN 'Sea'
    WHEN i94mode = 3 THEN 'Land'
  ELSE
  'Not reported'
END
  ARRIVAL_MODE,
  STATE_ID DESTINATION_STATE_ID,
  STATE_NAME DESTINATION_STATE,
  DATE_ADD('1960-1-1', INTERVAL CAST(DEPDATE AS INT64) DAY) DEPARTURE_DATE,
  CAST(i94bir AS numeric) AGE,
  CASE
    WHEN I94VISA = 1 THEN 'Business'
    WHEN i94visa = 2 THEN 'Pleasure'
    WHEN i94visa = 3 THEN 'Student'
END
  VISA_CATEGORY,
  matflag MATCH_FLAG,
  CAST(biryear AS numeric) BIRTH_YEAR,
  CASE
    WHEN gender = 'F' THEN 'FEMALE'
    WHEN GENDER = 'M' THEN 'MALE'
  ELSE
  'UNKNOWN'
END
  GENDER,
  insnum INS_NUMBER,
  AIRLINE,
  CAST(ADMNUM AS numeric) ADMISSION_NUMBER,
  FLTNO FLIGHT_NUMBER,
  VISATYPE VISA_TYPE
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.immigration_data` id
LEFT JOIN
  `{{ params.dwh_dataset }}.D_COUNTRY` DC
ON
  DC.COUNTRY_ID = I94RES
LEFT JOIN
  `{{ params.dwh_dataset }}.D_PORT` DP
ON
  DP.PORT_ID = i94port
LEFT JOIN
  `{{ params.dwh_dataset }}.D_STATE` DS
ON
  DS.STATE_ID = i94addr
