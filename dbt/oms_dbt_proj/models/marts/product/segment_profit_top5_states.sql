with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

top_states as (
    select state
    from {{ ref('top_states_by_revenue_profit') }}
),

filtered as (
    select *
    from enriched
    where state in (select state from top_states)
),

segment_profit as (
    select
        state,
        category,
        sum(profit) as total_profit
    from filtered
    group by state, category
)

select * from segment_profit
