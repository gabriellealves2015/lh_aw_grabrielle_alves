with
    sales_reason as (
        select
            id_sales_order
            , id_sales_reason
            , reason_name
            , reason_type
        from {{ ref('int_sales_order_reason') }}
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_sales_order', 'id_sales_reason']) }} as sk_order_reason
            , id_sales_order
            , id_sales_reason
            , reason_name
            , reason_type
        from sales_reason
    )

    select *
    from transformation
