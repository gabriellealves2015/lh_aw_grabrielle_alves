with
    sales_reason as (
        select
            id_sales_order
            , id_sales_reason
            , reason_name
            , reason_type
        from {{ ref('int_sales_order_reason') }}
    )

    , orders as (
        select distinct
            sk_sales_order
            , id_sales_order
        from {{ ref('fact_order_details') }}
    )

    , join_tables as (
        select
            sales_reason.id_sales_order
            , orders.sk_sales_order
            , sales_reason.id_sales_reason
            , sales_reason.reason_name
            , sales_reason.reason_type
        from sales_reason
        left join orders
            on sales_reason.id_sales_order = orders.id_sales_order
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_sales_order', 'id_sales_reason']) }} as sk_order_reason
            , sk_sales_order as fk_sales_order 
            , id_sales_order
            , id_sales_reason
            , reason_name
            , reason_type
        from join_tables
    )

    select *
    from transformation
