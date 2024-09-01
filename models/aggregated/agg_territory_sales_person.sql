with
    fact_sales_order as (
        select
            sk_order_details
            , sk_sales_order
            , fk_product  
            , fk_customer
            , fk_creditcard
            , fk_territory
            , fk_employee
            , tax
            , freight
            , due
            , is_online
            , order_date
            , order_quantity
            , subtotal
            , total

        from {{ ref('fact_order_details') }}
    )

    , dim_products as (
        select    
            sk_product
            , product_name
            , category_name 
            , subcategory_name

        from {{ ref('dim_products') }} 
    )

    , dim_customers as (
        select    
            sk_customer
            , full_name
            , person_type

        from {{ ref('dim_customers') }} 
    )

    , dim_territories as (
        select    
            sk_territory
            , city_name
            , state_province_name
            , country_name

        from {{ ref('dim_territories') }} 
    )

    , dim_creditcards as (
        select    
            sk_creditcard
            , card_type

        from {{ ref('dim_creditcards') }} 
    )

    , dim_employees as (
        select    
            sk_employee
            , full_name
            , job_title

        from {{ ref('dim_employees') }} 
    )

    , join_tables as (
        select
            fso.sk_sales_order
            , fso.sk_order_details
            , fso.fk_product
            , dpr.product_name
            , dpr.category_name 
            , dpr.subcategory_name 
            , fso.fk_customer
            , dcu.full_name as customer_full_name
            , dcu.person_type
            , fso.fk_creditcard
            , dcc.card_type
            , fso.fk_territory
            , dte.city_name
            , dte.state_province_name
            , dte.country_name
            , fso.fk_employee
            , dem.full_name as employee_full_name
            , dem.job_title
            , fso.tax
            , fso.freight
            , fso.due
            , fso.is_online
            , fso.order_date
            , fso.order_quantity
            , fso.subtotal
            , fso.total

        from fact_sales_order as fso
        left join dim_products as dpr
            on fso.fk_product = dpr.sk_product
        left join dim_customers as dcu
            on fso.fk_customer = dcu.sk_customer
        left join dim_creditcards as dcc
            on fso.fk_creditcard = dcc.sk_creditcard
        left join dim_territories as dte
            on fso.fk_territory = dte.sk_territory
        left join dim_employees as dem
            on fso.fk_employee = dem.sk_employee
    )

    , transformation as (
        select
            sk_order_details
            , sk_sales_order
            , fk_product
            , product_name
            , category_name 
            , subcategory_name 
            , fk_customer
            , customer_full_name
            , person_type
            , fk_creditcard
            , card_type
            , fk_territory
            , city_name
            , state_province_name
            , country_name
            , fk_employee
            , employee_full_name
            , job_title
            , tax
            , freight
            , due
            , is_online
            , case
                when is_online = True then 'Physical'
                else 'On-line'
            end as order_type
            , order_date
            , order_quantity
            , subtotal
            , total
        from join_tables
    )

select *
from transformation