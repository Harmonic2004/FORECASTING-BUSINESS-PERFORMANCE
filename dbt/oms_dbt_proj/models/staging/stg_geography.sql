{{ config(materialized='view') }}

SELECT
    CAST(Zip AS STRING) AS zip,
    City,
    State,
    Region,
    District
FROM {{ source('raw', 'geography') }}
