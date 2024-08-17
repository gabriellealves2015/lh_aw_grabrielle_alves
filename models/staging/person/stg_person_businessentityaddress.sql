with
    businessentityaddress as (
        select *
        from {{ source('person', 'businessentityaddress') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(businessentityid as integer) as string) as id_business_entity
            , cast(cast(addressid as integer) as string) as id_address
            , cast(cast(addresstypeid as integer) as string) as id_address_type

            /* Information */
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from businessentityaddress
    )

select *
from casting