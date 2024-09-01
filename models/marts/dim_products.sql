with
    product as (
        select
            id_product

            {{ generate_not_informed_case(['id_product_subcategory']) }}
            
            , name as product_name
        from {{ ref('stg_production_product') }}

        /* Define not informed */
        {% set product_fields = ['id_product', 'id_product_subcategory', 'product_name'] -%}
        {{- generate_not_informed_union(product_fields, ['id_product', 'id_product_subcategory']) }}

    )

    , category as (
        select
            id_product_category
            , name as category_name
        from {{ ref('stg_production_productcategory') }}

        /* Define not informed */
        {{ generate_not_informed_union(['id_product_subcategory', 'category_name'], ['id_product_subcategory']) }}

    )

    , subcategory as (
        select
            id_product_subcategory
            
            {{ generate_not_informed_case(['id_product_category']) }}
            
            , name as subcategory_name
        from {{ ref('stg_production_productsubcategory') }}

        /* Define not informed */
        {{ generate_not_informed_union(['id_product_subcategory', 'id_product_category', 'subcategory_name'], ['id_product_subcategory', 'id_product_category']) }}

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
