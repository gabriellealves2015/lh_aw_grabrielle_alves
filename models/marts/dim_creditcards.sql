with
    creditcard as (
        select
            id_creditcard
            , card_type
        from {{ ref('stg_sales_creditcard') }}
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_creditcard']) }} as sk_creditcard
            , id_creditcard
            , card_type
        from creditcard
    )

    select *
    from transformation
