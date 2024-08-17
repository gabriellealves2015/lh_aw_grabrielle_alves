with
    product_subcategory as (
        select *
        from {{ source('production', 'productsubcategory') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(productsubcategoryid as integer) as string) as id_product_subcategory
            , cast(cast(productcategoryid as integer) as string) as id_product_category

            /* Information */
            , cast(name as string) as name
            , cast(rowguid as string) as rowguid
            
            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from product_subcategory
    )

select *
from casting