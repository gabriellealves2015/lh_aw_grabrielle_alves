with
    person as (
        select *
        from {{ source('person', 'person') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(businessentityid as integer) as string) as id_business_entity

            /* Information */
            , cast(persontype as string) as person_type
            , cast(namestyle as boolean) as has_name_style
            , cast(title as string) as title
            , cast(firstname as string) as first_name
            , cast(middlename as string) as middle_name
            , cast(lastname as string) as last_name
            , cast(suffix as string) as suffix
            , cast(emailpromotion as integer) as email_promotion
            , cast(additionalcontactinfo as string) as additional_contact_info
            , cast(demographics as string) as demographics
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from person
    )

select *
from casting