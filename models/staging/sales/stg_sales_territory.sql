with
    store as (
        select *
        from {{ source('sales', 'salesterritory') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(territoryid as integer) as string) as id_territory

            /* Information */
            , cast(name as string) as territory_name
            , cast('group' as string) as territory_group
            , cast(countryregioncode as string) as country_region_code
            , cast(salesytd as integer) as sales_year_to_date
            , cast(saleslastyear as integer) as sales_last_year
            , cast(costytd as integer) as cost_year_to_date
            , cast(costlastyear as integer) as cost_last_year
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from store
    )

select *
from casting
