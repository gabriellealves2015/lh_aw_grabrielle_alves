with
    product as (
        select
            id_product
            , id_product_subcategory
            , name as product_name
        from {{ ref('stg_production_product') }}
    )

    , category as (
        select
            id_product_category
            , name as category_name
        from {{ ref('stg_production_productcategory') }}
    )

    , subcategory as (
        select
            id_product_subcategory
            , id_product_category
            , name as subcategory_name
        from {{ ref('stg_production_productsubcategory') }}
    )

    , join_tables as (
        select
            product.id_product
            , subcategory.id_product_category
            , subcategory.id_product_subcategory
            , product.product_name
            , category.category_name 
            , subcategory.subcategory_name    
        from product
        left join subcategory
            on product.id_product_subcategory = subcategory.id_product_subcategory
        left join category
            on category.id_product_category = subcategory.id_product_category
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_product']) }} as sk_product
            , id_product
            , id_product_category
            , id_product_subcategory
            , product_name
            , category_name 
            , subcategory_name   
        from join_tables
    )

    select *
    from transformation
