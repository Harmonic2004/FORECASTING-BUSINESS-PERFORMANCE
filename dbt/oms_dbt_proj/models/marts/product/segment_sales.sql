with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

segment_sales as (

    select
        segment,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    group by segment

)

select * from segment_sales
