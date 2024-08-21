with
    sales_reason as (
        select
            id_sales_reason
            , name as reason_name
            , reason_type
        from {{ ref('stg_sales_salesreason') }}
    )

    , sales_order_reason as (
        select
            id_sales_order
            , id_sales_reason
        from {{ ref('stg_sales_salesorderheadersalesreason') }}
    )

    , transformation as (
        select
            saord.id_sales_order
            , sare.id_sales_reason
            , sare.reason_name
            , sare.reason_type
        from sales_order_reason as saord
        left join sales_reason as sare
            on saord.id_sales_reason = sare.id_sales_reason
    )

    , agg_sales_reason as (
        select
            id_sales_order
            , string_agg(id_sales_reason, ', ') as aggregated_id_sales_reason
            , string_agg(reason_name, ', ') as aggregated_reason_name
            , string_agg(reason_type, ', ') as aggregated_reason_type
        from transformation
        group by
            id_sales_order
    )

    select *
    from agg_sales_reason