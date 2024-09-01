with
    sales_order as (
        select
            id_sales_order
            , id_customer
            , id_sales_person
            , id_territory
            , id_creditcard
            , id_ship_to_address
            , status
            , subtotal
            , tax_amt
            , freight
            , total_due
            , is_online
            , order_date
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , sales_order_detail as (
        select
            id_sales_order_detail
            , id_sales_order
            , id_product
            , order_quantity
            , unit_price
            , unit_price_discount
        from {{ ref('stg_sales_salesorderdetail') }}
    )

    , count_items_in_each_order as (
        select
            id_sales_order
            , count(id_sales_order_detail) as total_items
        from sales_order_detail
        group by id_sales_order
    )

    , join_tables as (
        select
            saor.id_sales_order
            , saorde.id_sales_order_detail
            , saor.id_customer
            , saor.id_sales_person
            , saor.id_territory
            , saor.id_creditcard
            , saor.id_ship_to_address
            , saorde.id_product
            , saor.status
            , saor.subtotal
            , saor.tax_amt
            , saor.freight
            , saor.total_due
            , saor.is_online
            , saor.order_date
            , saorde.order_quantity
            , saorde.unit_price
            , saorde.unit_price_discount
            , coun.total_items
        from sales_order as saor
        left join sales_order_detail as saorde 
            on saor.id_sales_order = saorde.id_sales_order
        left join count_items_in_each_order as coun
            on coun.id_sales_order = saor.id_sales_order
    )

    , transformation as (
        select
            id_sales_order
            , id_sales_order_detail
            , id_customer
            , id_sales_person
            , id_territory
            , id_creditcard
            , id_ship_to_address
            , id_product
            , status
            , (tax_amt / total_items) as tax
            , (freight / total_items) as freight
            , (total_due / total_items) as due
            , is_online
            , order_date
            , order_quantity
            , unit_price
            , unit_price_discount
            , (unit_price * order_quantity * (1 - unit_price_discount)) as subtotal
        from join_tables
    )

    , calculation as (
        select
            id_sales_order
            , id_sales_order_detail
            , id_customer
            , id_sales_person
            , id_territory
            , id_creditcard
            , id_ship_to_address
            , id_product
            , status
            , tax
            , freight
            , due
            , is_online
            , order_date
            , order_quantity
            , unit_price
            , unit_price_discount
            , subtotal
            , (subtotal + tax + freight) as total
        from transformation
    )

select *
from calculation