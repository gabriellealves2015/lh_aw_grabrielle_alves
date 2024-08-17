with
    customer as (
        select *
        from {{ source('sales', 'customer') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(customerid as integer) as string) as id_customer
            , cast(cast(personid as integer) as string) as id_person
            , cast(cast(storeid as integer) as string) as id_store
            , cast(cast(territoryid as integer) as string) as id_territory

            /* Information */
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from customer
    )

select *
from casting
