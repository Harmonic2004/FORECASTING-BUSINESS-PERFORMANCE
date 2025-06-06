{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ProductID', 'Date', 'Zip']) }} AS sales_id,
    CAST(ProductID AS STRING) AS product_id,
    CAST(Date AS DATE) AS sale_date,
    CAST(Zip AS STRING) AS zip,
    CAST(Units AS INT) AS units,
    CAST(Revenue AS FLOAT) AS revenue,
    CAST(COGS AS FLOAT) AS cogs,
    (Revenue - COGS) AS profit
FROM {{ source('raw', 'salesfact') }}
