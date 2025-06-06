with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

state_agg as (

    select
        state,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    group by state

)

select * from state_agg