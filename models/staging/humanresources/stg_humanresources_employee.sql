with
    employee as (
        select *
        from {{ source('humanresources', 'employee') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(businessentityid as integer) as string) as id_business_entity
            , cast(loginid as string) as id_login

            /* Information */
            , cast(nationalidnumber as string) as national_identification_number
            , cast(jobtitle as string) as job_title
            , cast(birthdate as timestamp) as birth_date
            , cast(hiredate as timestamp) as hire_date
            , case
                when lower(cast(maritalstatus as string)) = 'm' then 'Married'
                when lower(cast(maritalstatus as string)) = 's' then 'Single'
            end as marital_status
            , case
                when lower(cast(gender as string)) = 'm' then 'Male'
                when lower(cast(gender as string)) = 's' then 'Female'
            end as gender
            , cast(vacationhours as integer) as vacation_hours
            , cast(sickleavehours as integer) as sick_leave_hours
            , cast(organizationnode as string) as organization_node
            , cast(rowguid as string) as rowguid

            /* Flags */
            , cast(salariedflag as boolean) as is_salaried
            , cast(currentflag as boolean) as is_current            

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from employee
    )

select *
from casting