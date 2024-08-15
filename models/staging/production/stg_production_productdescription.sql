with
    product as (
        select *
        from {{ source('production', 'productdescription') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(productdescriptionid as integer) as string) as id_product_description

            /* Information */
            , cast(description as string) as description
            , cast(rowguid as string) as rowguid
            
            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from product
    )

select *
from casting