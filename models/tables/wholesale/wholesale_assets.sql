{{
  config({
    "materialized" : "table",
    "unique_key" : "unique_id"
    })
}}

select
	A.unique_id,
	A.account_id,
	A.sku, 
	A.description,
	A.date_purchased, 
	A.serial_number,
	A.value,
	A.notes, 
	A.status,
	A.date_moved, 
	A.vendor,
	A.class,  
	a.invested_value,
	a.customer_code,
	a.location

from {{ref('warehouse_assets')}} a