with
    sales_order as (
        select
            id_sales_order
            , id_sales_person
        from {{ ref('stg_sales_salesorderheader') }}
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

    , employee as (
        select
            id_business_entity
            , job_title
        from {{ ref('stg_humanresources_employee') }}
    )

    , join_tables as (
        select
            sales_order.id_sales_order
            , sales_order.id_sales_person 
            , person.first_name 
            , person.middle_name 
            , person.last_name
            , person.full_name
            , employee.job_title
        from sales_order
        left join person
            on sales_order.id_sales_person = person.id_business_entity
        left join employee
            on employee.id_business_entity = person.id_business_entity
    )

    , transformation as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_sales_order']) }} as sk_employee
            , id_sales_order
            , id_sales_person
            , case
                when first_name is null 
                    and middle_name is null 
                    and last_name is null
                    then 'Not informed'
                else
                    first_name
            end as first_name
            , middle_name
            , last_name
            , case
                when trim(full_name) = '' then 'Not informed'
                else
                    full_name
            end as full_name
            , job_title
        from join_tables
    )

select *
from transformation