{{ config(materialized='view') }}
select * from {{ ref('stg_chicago_crimes') }}
