with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

yearly as (

    select
        date_trunc('year', sale_date) as year,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    where sale_date between '2010-08-01' and '2022-06-30'
    group by date_trunc('year', sale_date)
)

select * from yearly
order by year
