{{
  config({
    "materialized" : "table",
    "sort" : "created_at",
    "unique_key" : "unique_id",
    "post-hook" : [
    	"drop table if exists {{ref('shopify_customers')}} cascade",
    	"drop table if exists {{ref('shopify_non_subscription_customers')}} cascade",
    	"drop table if exists {{ref('shopify_subscription_customers')}} cascade",
    	"drop table if exists {{ref('shopify_orders')}} cascade",
    	"drop table if exists {{ref('shopify_non_subscription_orders')}} cascade",
    	"drop table if exists {{ref('shopify_subscription_orders')}} cascade",
    	"drop table if exists {{ref('shopify_order_items')}} cascade",
    	"drop table if exists {{ref('shopify_non_subscription_order_items')}} cascade",
    	"drop table if exists {{ref('shopify_subscription_order_items')}} cascade",
    	"drop table if exists {{ref('square_cafe_order_items')}} cascade",
    	"drop table if exists {{ref('square_cafe_orders')}} cascade"

    	]
    })
}}

select

--IDs
	md5('DTC' || id) as unique_id,
	'DTC' as source,
	id::varchar,
	order_id::varchar,
	customer_id::varchar,

--Item info
	sku,
	name as item_name,
	variant_title as variant_name,

	product_type,
	price,
	quantity,
	line_item_discount,
	line_item_net_sales,
	line_item_gross_sales,
	line_item_weight,
	weight,
	financial_status,

--Customer Info
	NULL as customer_name,
	 case
		when customer_order_number > 1 then 'Repeat'
		else 'First'
	end as unit_type,
 	subscription_type as unit_sub_type,

--Location
	NULL as region,

--Calendar
	created_at,
	created_at_date,
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

from {{ref('dtc_order_items')}}

UNION

select

--IDs
	md5('Retail' || unique_id) as unique_id,
	'Retail' as source,
	unique_id as id,
	order_id,
	NULL as customer_id,

-- Item info
	sku,
	item_name,
	variant_name,
	NULL as product_type,
	item_base_price as price,
	quantity,
	discounts as line_item_discount,
	net_sales as line_item_net_sales,
	gross_sales as line_item_gross_sales,
	NULL as line_item_weight,
	NULL as weight,
	NULL as financial_status,

--Customer Info
	location_code as customer_name,
	is_comp as unit_type,
	location_code as unit_sub_type,

--Location
	region,

--Calendar
	created_at,
	created_at_date,
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

from {{ref('retail_order_items')}}

UNION

SELECT 
--IDs
	md5('Wholesale' || unique_invoice_item_id) as unique_id,
	'Wholesale' as source,
	unique_invoice_item_id as id,
	unique_invoice_id as order_id,
	customer_code as customer_id,

--Item info
	sku,
	item_name,
	NULL as variant_name,
	item_type as product_type,
	unit_price as price,
	quantity,
	NULL as line_item_discount,
	extension as line_item_net_sales,
	NULL as line_item_gross_sales,
	total_weight as line_item_weight,
	NULL as weight,
	NULL as financial_status,

--Customer Info

	account_name as customer_name,
	customer_type as unit_type,
	case 
		when customer_type = 'Hospitality' then region
		when customer_type = 'CPG' then account_name
		else NULL
	end as unit_sub_type,

--Location
	region,
--Calendar
	ship_date,
	ship_date,
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

from {{ref('wholesale_invoice_items')}}


