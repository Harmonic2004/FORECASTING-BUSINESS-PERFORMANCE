with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

monthly as (

    select
        last_day(sale_date) as month,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    where sale_date between '2010-08-01' and '2022-06-30'
    group by last_day(sale_date)
)

select * from monthly order by month
