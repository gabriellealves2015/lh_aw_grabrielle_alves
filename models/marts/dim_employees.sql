with
    sales_order as (
        select
            id_sales_order
            , case
                /* Define as on-line */
                when id_sales_person is null and is_online = true then '9999998'
                
                /* Define as not informed */
                when id_sales_person is null then '9999999'

                else id_sales_person
            end id_sales_person
            , is_online
        from {{ ref('stg_sales_salesorderheader') }}
    )

    , person as (
        select
            id_business_entity
            , first_name
            , middle_name
            , last_name
            , concat(ifnull(first_name, ''), ' ', ifnull(middle_name, ''), ' ', ifnull(last_name, '')) as full_name
        from {{ ref('stg_person_person') }}
        
        /* Define on-line */
        union all
        select
            '9999998' as id_business_entity
            , 'On-line' as first_name
            , null as middle_name
            , null as last_name
            , 'On-line' as full_name

        /* Define not informed */
        {% set person_fields = ['id_business_entity', 'first_name', 'middle_name', 'last_name', 'full_name'] -%}
        {{- generate_not_informed_union(person_fields, ['id_business_entity'], ['middle_name', 'last_name']) }}

    )

    , employee as (
        select
            id_business_entity
            , job_title
        from {{ ref('stg_humanresources_employee') }}
        
        /* Define not informed */
        {{ generate_not_informed_union(['id_business_entity', 'job_title'], ['id_business_entity']) }}

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
            , sales_order.is_online
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
            , first_name
            , middle_name
            , last_name
            , full_name
            , job_title
            , is_online
        from join_tables
    )

select *
from transformation