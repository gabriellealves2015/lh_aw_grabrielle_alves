with
    countryregion as (
        select *
        from {{ source('person', 'countryregion') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(countryregioncode as string) as country_region_code

            /* Information */
            , cast(name as string) as name

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from countryregion
    )

select *
from casting