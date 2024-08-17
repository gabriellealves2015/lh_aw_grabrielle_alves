with
    creditcard as (
        select *
        from {{ source('sales', 'creditcard') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(creditcardid as integer) as string) as id_creditcard

            /* Information */
            , cast(cardtype as string) as card_type
            , cast(cardnumber as string) as card_number
            , cast(expmonth as integer) as expiration_month
            , cast(expyear as integer) as expiration_year
            

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from creditcard
    )

select *
from casting
