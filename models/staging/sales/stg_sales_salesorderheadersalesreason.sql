with
    salesorderheadersalesreason as (
        select *
        from {{ source('sales', 'salesorderheadersalesreason') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(salesorderid as integer) as string) as id_sales_order
            , cast(cast(salesreasonid as integer) as string) as id_sales_reason

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from salesorderheadersalesreason
    )

select *
from casting
