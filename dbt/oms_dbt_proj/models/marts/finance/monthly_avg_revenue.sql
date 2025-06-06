with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

monthly as (
    select
        extract(month from sale_date) as month,
        avg(revenue) as avg_monthly_revenue
    from enriched
    where sale_date between '2010-01-01' and '2022-12-31'
    group by 1

)

select * from monthly order by month
