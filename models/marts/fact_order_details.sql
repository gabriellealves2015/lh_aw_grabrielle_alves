with
    territories as (
        select
            sk_territory
            , id_ship_to_address
        from {{ ref('dim_territories') }}
    )

    , products as (
        select
            sk_product
            , id_product
        from {{ ref('dim_products') }}
    )

    , customers as (
        select
            sk_customer
            , id_customer
        from {{ ref('dim_customers') }} 
    )

    , order_reasons as (
        select
            sk_order_reason
            , id_sales_order
            , aggregated_reason_name
        from {{ ref('dim_order_reasons') }}
    )

    , creditcards as (
        select
            sk_creditcard
            , id_creditcard
        from {{ ref('dim_creditcards') }}
    )

    , employees as (
        select
            sk_employee
            , id_sales_order
            , id_sales_person
        from {{ ref('dim_employees') }}
    )

    , sales_orders as (
        select
            id_sales_order
            , id_customer
            , id_sales_person
            , id_territory
            , id_product
            , id_creditcard
            , id_ship_to_address
            , id_sales_order_detail
            , status
            , subtotal
            , tax_amt
            , freight
            , total_due
            , is_online
            , order_date
            , order_quantity
            , unit_price
            , unit_price_discount
            , total
        from {{ ref('int_sales_order') }}
    )

    , join_tables as (
        select
            orre.sk_order_reason
            , saor.id_sales_order
            , saor.id_sales_order_detail
            , pro.sk_product
            , saor.id_product
            , cus.sk_customer
            , saor.id_customer
            , cred.sk_creditcard
            , saor.id_creditcard
            , terr.sk_territory
            , saor.id_ship_to_address
            , emp.sk_employee
            , saor.id_sales_person
            , saor.status
            , saor.subtotal
            , saor.tax_amt
            , saor.freight
            , saor.total_due
            , saor.is_online
            , saor.order_date
            , saor.order_quantity
            , saor.unit_price
            , saor.unit_price_discount
            , saor.total
        from sales_orders as saor
        left join products as pro 
            on pro.id_product = saor.id_product 
        left join order_reasons as orre
            on orre.id_sales_order = saor.id_sales_order
        left join customers as cus
            on cus.id_customer = saor.id_customer
        left join creditcards as cred
            on cred.id_creditcard = saor.id_creditcard
        left join territories as terr 
            on terr.id_ship_to_address = saor.id_ship_to_address
        left join employees as emp
            on emp.id_sales_order = saor.id_sales_order
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_sales_order', 'id_sales_order_detail']) }} as sk_order_details
            , id_sales_order
            , sk_order_reason as fk_order_reason
            , sk_product as fk_product
            , id_product
            , sk_customer as fk_customer
            , id_customer
            , sk_creditcard as fk_creditcard
            , id_creditcard
            , sk_territory as fk_territory
            , id_ship_to_address
            , sk_employee as fk_employee
            , id_sales_person
            , status
            , subtotal
            , tax_amt
            , freight
            , total_due
            , is_online
            , order_date
            , order_quantity
            , unit_price
            , unit_price_discount
            , total
        from join_tables
    )

select *
from transformation

    