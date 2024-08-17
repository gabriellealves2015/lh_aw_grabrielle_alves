with
    store as (
        select *
        from {{ source('sales', 'store') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(businessentityid as integer) as string) as id_business_entity
            , cast(cast(salespersonid as integer) as string) as id_sales_person

            /* Information */
            , cast(name as string) as name
            , cast(demographics as string) as demographics
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from store
    )

select *
from casting
