with
    customer as (
        select
            id_customer
            , id_person
            , id_store
            , id_territory
        from {{ ref('stg_sales_customer') }}
    )

    , person as (
        select
            id_business_entity
            , first_name
            , middle_name
            , last_name
            , concat(ifnull(first_name,''), ' ', ifnull(middle_name, ''), ' ', ifnull(last_name, '')) as full_name
        from {{ ref('stg_person_person') }}
    )

    , store as (
        select
            id_business_entity
            , name
        from {{ ref('stg_sales_store') }}
    )

    , join_tables as (
        select
            customer.id_customer
            , customer.id_person
            , person.first_name
            , person.middle_name
            , person.last_name
            , person.full_name
            , customer.id_store
            , store.name as store_name
        from customer
        left join person
            on customer.id_person = person.id_business_entity
        left join store
            on customer.id_store = store.id_business_entity
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_customer']) }} as sk_customer
            , id_customer
            , id_person
            , id_store
            , first_name
            , middle_name
            , last_name
            , full_name
            , store_name
            , case 
                when id_person is null and id_store is not null then 'Store'
                when id_person is not null and id_store is null then 'Natural Person'
                when id_person is not null and id_store is not null then 'Store Contact'
            end as person_type
        from join_tables
    )

select *
from transformation