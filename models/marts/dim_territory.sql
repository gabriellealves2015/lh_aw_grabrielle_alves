with
    state_province as (
        select
            id_state_province
            , id_territory
            , state_province_code
            , country_region_code
            , name as state_province_name
        from {{ ref('stg_person_stateprovince') }}
    )

    , country_region as (
        select
            country_region_code
            , name as country_name
        from {{ ref('stg_person_countryregion') }}
    )

    , ship_address as (
        select
            distinct id_ship_to_address
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , address as (
        select
            id_address
            , id_state_province
            , city as city_name
        from {{ ref('stg_person_address') }}
    )

    , join_tables as (
        select
            ship_address.id_ship_to_address
            , state_province.id_state_province
            , state_province.id_territory
            , address.city_name
            , state_province.state_province_name
            , country_region.country_name
        from ship_address
        left join address
            on ship_address.id_ship_to_address = address.id_address
        left join state_province
            on state_province.id_state_province = address.id_state_province
        left join country_region
            on country_region.country_region_code = state_province.country_region_code
    )
    
    , transformation as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_ship_to_address']) }} as sk_territory
            , id_ship_to_address
            , id_state_province
            , id_territory
            , city_name
            , state_province_name
            , country_name
        from join_tables
    )

select *
from transformation