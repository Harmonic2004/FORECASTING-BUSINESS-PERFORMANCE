with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

region_sales as (

    select
        region,
        sum(revenue) as total_revenue
    from enriched
    group by region

)

select * from region_sales order by total_revenue desc
