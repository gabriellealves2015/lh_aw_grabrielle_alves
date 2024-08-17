with
    salesreason as (
        select *
        from {{ source('sales', 'salesreason') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(salesreasonid as integer) as string) as id_sales_reason
            
            /* Information */
            , cast(name as string) as name
            , cast(reasontype as string) as reason_type

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from salesreason
    )

select *
from casting
