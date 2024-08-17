with
    address as (
        select *
        from {{ source('person', 'address') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(addressid as integer) as string) as id_address
            , cast(cast(stateprovinceid as integer) as string) as id_state_province
            , cast(postalcode as string) as postal_code

            /* Information */
            , cast(addressline1 as string) as address_line_1
            , cast(addressline2 as string) as address_line_2
            , cast(city as string) as city
            , cast(spatiallocation as string) as spatial_location
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from address
    )

select *
from casting