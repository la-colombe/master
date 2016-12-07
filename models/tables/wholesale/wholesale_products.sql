{{
  config({
    "materialized" : "table",
    "sort" : "created_at",
    "unique_key" : "sku"
    })
}}

select
sku, 
created_at,
name, 
product_line, 
unit_price, 
product_type,
sales_unit_of_measure, 
volume, 
ship_weight,  
commissionable,
tax_class, 
status, 
unit_cost, 
vendor

from {{ref('warehouse_products')}}