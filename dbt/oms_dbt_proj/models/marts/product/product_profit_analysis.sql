with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

product_profit as (
    select
        product_id,
        product,
        sum(profit) as total_profit
    from enriched
    group by product_id, product
    order by total_profit desc
)

select * from product_profit
