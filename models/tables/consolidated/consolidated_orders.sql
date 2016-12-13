{{
  config({
    "materialized" : "table",
    "sort" : "created_at",
    "unique_key" : "unique_id",
    })
}}

Select
--IDs
	md5('DTC' || id) as unique_id,
	'DTC' as source,
	id::varchar as order_id,
	customer_id::varchar,

--Item info
	
	gross_sales,
	net_sales,
	NULL as discounts,
	1 as units,

--Unit Info
	NULL as region,
	NULL as name,
	NULL as is_comp,
	NULL as unit_type,

--Calendar

	created_at,
	NULL as created_at_hour,
	created_at_fday,
	created_at_fweek,
	created_at_fperiod,
	created_at_fyear,
	created_at_fquarter,
	created_at_fday_of_week,
	created_at_fday_of_period,
	is_last_week,
	is_last_month,
	is_ty,
	is_ly,
	is_wtd,
	is_mtd,
	is_qtd,
	is_ytd

from {{ref('dtc_orders')}}

UNION

select
--IDs
	md5('Retail' || id) as unique_id,
	'Retail' as source,
	id as order_id,
	NULL as customer_id,

	gross_sales,
	net_sales,
	discounts,
	units,

-- Unit Info
	region,
	location_code as name,
	is_comp,
	location_code as unit_type,

--Retail Calendar
	created_at,
	created_at_hour,
	created_at_fday,
	created_at_fweek,
	created_at_fperiod,
	created_at_fyear,
	created_at_fquarter,
	created_at_fday_of_week,
	created_at_fday_of_period,
	is_last_week,
	is_last_month,
	is_ty,
	is_ly,
	is_wtd,
	is_mtd,
	is_qtd,
	is_ytd

from {{ref('retail_orders')}}

UNION

SELECT 

--IDs
	md5('Wholesale' || unique_invoice_id) as unique_id,
	'Wholesale' as source,
	unique_invoice_id as order_id,
	customer_code as customer_id,

  	total_extension as gross_sales,
  	total_extension as net_sales,
  	NULL as discounts,
  	total_weight as units,

  	region,
  	account_name as name,
  	NULL as is_comp,
 	customer_type as unit_type,
 
	transaction_date,
	NULL,
	transaction_date_fday,
	transaction_date_fweek,
	transaction_date_fperiod,
	transaction_date_fyear,
	transaction_date_fquarter,
	transaction_date_fday_of_week,
	transaction_date_fday_of_period,
	is_last_week,
	is_last_month,
	is_ty,
	is_ly,
	is_wtd,
	is_mtd,
	is_qtd,
	is_ytd

from {{ref('wholesale_invoices')}}