with
    product_category as (
        select *
        from {{ source('production', 'productcategory') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(productcategoryid as integer) as string) as id_product_category

            /* Information */
            , cast(name as string) as name
            , cast(rowguid as string) as rowguid
            
            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from product_category
    )

select *
from casting