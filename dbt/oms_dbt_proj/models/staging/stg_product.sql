{{ config(materialized='view') }}

SELECT
    CAST(ProductID AS STRING) AS product_id,
    Category,
    Segment,
    Product
FROM {{ source('raw', 'product') }}
