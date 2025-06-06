with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

category_sales as (

    select
        category,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    group by category

)

select * from category_sales
