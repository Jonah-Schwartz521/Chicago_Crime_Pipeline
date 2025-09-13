{{ config(materialized='view') }}

WITH src AS (
  SELECT
    source_id,
    payload ->> 'case_number'          AS case_number,
    payload ->> 'primary_type'         AS primary_type,
    payload ->> 'arrest'               AS arrest_txt,
    payload ->> 'domestic'             AS domestic_txt,
    payload ->> 'district'             AS district_txt,
    payload ->> 'community_area'       AS community_area_txt,
    payload ->> 'location_description' AS location_description,
    payload ->> 'latitude'             AS latitude_txt,
    payload ->> 'longitude'            AS longitude_txt,
    payload ->> 'date'                 AS date_txt,
    payload ->> 'updated_on'           AS updated_on_txt
  FROM raw.chicago_crimes
)
SELECT
  source_id AS id,
  case_number,
  primary_type,
  NULLIF(LOWER(arrest_txt),   '')::boolean   AS arrest,
  NULLIF(LOWER(domestic_txt), '')::boolean   AS domestic,
  NULLIF(district_txt, '')::int              AS district,
  NULLIF(community_area_txt, '')::int        AS community_area,
  location_description,
  NULLIF(latitude_txt, '')::numeric          AS latitude,
  NULLIF(longitude_txt, '')::numeric         AS longitude,
  NULLIF(date_txt, '')::timestamp            AS date,
  NULLIF(updated_on_txt, '')::timestamp      AS updated_on
FROM src
