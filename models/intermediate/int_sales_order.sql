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

    , transformation as (
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
            , (saorde.unit_price * saorde.order_quantity * (1 - unit_price_discount)) as total
        from sales_order as saor
        left join sales_order_detail as saorde 
            on saor.id_sales_order = saorde.id_sales_order
    )

    select *
    from transformation