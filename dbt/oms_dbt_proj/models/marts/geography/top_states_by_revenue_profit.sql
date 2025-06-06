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

),

ranked as (

    select *,
           rank() over (order by total_revenue desc) as rank_revenue
    from state_agg

)

select * from ranked
where rank_revenue <= 5 or rank_revenue >= (select max(rank_revenue) - 4 from ranked)
order by total_revenue desc
