with
    sales_reason as (
        select
            id_sales_order
            , aggregated_id_sales_reason
            , aggregated_reason_name
            , aggregated_reason_type
        from {{ ref('int_sales_order_reason') }}
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_sales_order']) }} as sk_order_reason
            , id_sales_order
            , aggregated_id_sales_reason
            , aggregated_reason_name
            , aggregated_reason_type
        from sales_reason
    )

    select *
    from transformation
