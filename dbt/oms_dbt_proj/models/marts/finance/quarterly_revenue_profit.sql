with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

quarterly as (

    select
        date_trunc('quarter', sale_date) as quarter,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit
    from enriched
    where sale_date between '2010-01-01' and '2022-12-31'
    group by 1

)

select * from quarterly order by quarter
