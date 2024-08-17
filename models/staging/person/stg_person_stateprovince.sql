with
    stateprovince as (
        select *
        from {{ source('person', 'stateprovince') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(stateprovinceid as integer) as string) as id_state_province
            , cast(cast(territoryid as integer) as string) as id_territory
            , cast(stateprovincecode as string) as state_province_code
            , cast(countryregioncode as string) as country_region_code

            /* Information */
            , cast(name as string) as name
            , cast(rowguid as string) as rowguid

            /* Flags */
            , cast(isonlystateprovinceflag as boolean) as is_only_state_province

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from stateprovince
    )

select *
from casting