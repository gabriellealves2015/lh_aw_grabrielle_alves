with
    sales_order_detail as (
        select *
        from {{ source('sales', 'salesorderdetail') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(salesorderdetailid as integer) as string) as id_sales_order_detail
            , cast(cast(salesorderid as integer) as string) as id_sales_order
            , cast(cast(productid as integer) as string) as id_product
            , cast(cast(specialofferid as integer) as string) as id_special_offer

            /* Information */
            , cast(carriertrackingnumber as string) as carrier_tracking_number
            , cast(orderqty as integer) as order_quantity
            , cast(unitprice as numeric) as unit_price
            , cast(unitpricediscount as numeric) as unit_price_discount
            , cast(rowguid as string) as rowguid

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date
        
       from sales_order_detail
    )

select *
from casting