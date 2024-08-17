with
    sales_order as (
        select *
        from {{ source('sales', 'salesorderheader') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(salesorderid as integer) as string) as id_sales_order
            , cast(cast(customerid as integer) as string) as id_customer
            , cast(cast(salespersonid as integer) as string) as id_sales_person
            , cast(cast(territoryid as integer) as string) as id_territory
            , cast(cast(billtoaddressid as integer) as string) as id_bill_to_address
            , cast(cast(shiptoaddressid as integer) as string) as id_ship_to_address
            , cast(cast(shipmethodid as integer) as string) as id_ship_method
            , cast(cast(creditcardid as integer) as string) as id_creditcard
            , cast(cast(currencyrateid as integer) as string) as id_currency_rate
            , cast(creditcardapprovalcode as string) as creditcard_approval_code

            /* Information */
            , cast(revisionnumber as integer) as revision_number
            , case
                when cast(status as integer) = 1 then 'In process'
                when cast(status as integer) = 2 then 'Approved'
                when cast(status as integer) = 3 then 'Backordered'
                when cast(status as integer) = 4 then 'Rejected'
                when cast(status as integer) = 5 then 'Shipped'
                else 'Cancelled'
            end as status
            , cast(purchaseordernumber as string) as purchase_order_number
            , cast(accountnumber as string) as account_number
            , cast(subtotal as numeric) as subtotal
            , cast(taxamt as numeric) as tax_amt
            , cast(freight as numeric) as freight
            , cast(totaldue as numeric) as total_due
            , cast(comment as string) as comment
            , cast(rowguid as string) as rowguid
            
            /* Flags */
            , cast(onlineorderflag as boolean) as is_online

            /* Dates Columns */
            , cast(orderdate as timestamp) as order_date
            , cast(duedate as timestamp) as due_date
            , cast(shipdate as timestamp) as ship_date

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
            
        from sales_order
    )

select *
from casting
