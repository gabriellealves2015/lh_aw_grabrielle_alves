with
    product as (
        select *
        from {{ source('production', 'product') }}
    )

    , casting as (
        select
            /* IDs and Codes */
            cast(cast(productid as integer) as string) as id_product
            , cast(cast(productsubcategoryid as integer) as string) as id_product_subcategory
            , cast(cast(productmodelid as integer) as string) as id_product_model
            
            /* Information */
            , cast(name as string) as name
            , cast(color as string) as color
            , cast(productnumber as string) as product_number
            , cast(safetystocklevel as integer) as safety_stock_level
            , cast(reorderpoint as integer) as reorder_point
            , cast(standardcost as numeric) as standard_cost
            , cast(listprice as numeric) as list_price
            , cast(size as string) as size
            , cast(sizeunitmeasurecode as string) as size_unit_measure_code
            , cast(weightunitmeasurecode as string) as weight_unit_measure_code
            , cast(weight as numeric) as weight
            , cast(daystomanufacture as integer) as days_to_manufacture
            , cast(productline as string) as product_line
            , cast(class as string) as class
            , cast(style as string) as style
            , cast(rowguid as string) as rowguid
            
            /* Flags */
            , cast(makeflag as boolean) as is_make
            , cast(finishedgoodsflag as boolean) as is_finished_goods

            /* Dates Columns */
            , cast(sellstartdate as timestamp) as sell_start_date
            , cast(sellenddate as timestamp) as sell_end_date
            , cast(cast(discontinueddate as string) as timestamp) as discontinued_date

            /* Control Columns - Table */
            , cast(modifieddate as timestamp) as modified_date

        from product
    )

select *
from casting