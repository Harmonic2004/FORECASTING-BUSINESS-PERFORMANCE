with sales as (

    select * from {{ ref('stg_salesfact') }}

),

geo as (

    select * from {{ ref('stg_geography') }}

),

prod as (

    select * from {{ ref('stg_product') }}

),

joined as (

    select
        s.sale_date,
        s.product_id,
        s.units,
        s.revenue,
        s.cogs,
        s.profit,
        g.city,
        g.state,
        g.region,
        g.district,
        p.category,
        p.segment,
        p.product
    from sales s
    left join geo g on s.zip = g.zip
    left join prod p on s.product_id = p.product_id

)

select * from joined
