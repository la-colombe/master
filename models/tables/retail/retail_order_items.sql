{{
  config({
    "materialized" : "table",
    "unique_key" : "unique_id",
    "sort" : "created_at",
    })
}}

select
	unique_id,
	convert_timezone(timezone, created_at) as created_at,
	gross_sales,
	net_sales,
	discounts,
	updated_at,
	order_id,

-- Item info
	sku,
	item_name,
	variant_name,
	category_name,
	quantity,
	item_base_price,
	employee,
	expired,
	notes,

--Location
	open_date,
	comp_date,
	cm.name as cafe_name,
	region,
	store_type,
	location_code,
	cm.is_comp,

--Retail Calendar
	date_trunc('day', convert_timezone(timezone, created_at)) as created_at_date,
	to_char(convert_timezone(timezone, created_at), 'HH24') as created_at_hour
      
from {{ref('square_cafe_order_items')}} o 
join {{ref('cafe_mapping')}} cm on o.location_code = cm.square

UNION

select 
	md5(pk) as unique_id,
	m.date as created_at,
	revenue as gross_sales,
	round(revenue * (1-rate), 2) as net_sales,
	null as discounts,
	m.date as updated_at,
	null as order_id,

-- Item info
	sku,
	item_name,
	item_name as variant_name,
	NULL as category_name,
	count as quantity,
	NULL as item_base_price,
	FALSE as employee,
	FALSE as expired,
	NULL as notes,
--Location
	open_date,
	comp_date,
	cr.name,
	region,
	store_type,
	location_code,
	cr.is_comp,
--Retail Calendar
	m.date as created_at_date,
	NULL as created_at_hour

from {{ref('micros_order_items')}} m
join 
  (
    select *
    from {{ref('cafe_mapping')}} cm
    join
    (
      select location_code, avg(taxes / nullif((gross_sales + inclusive_tax),0)) as rate, min(date_trunc('day', created_at)) as square_start_date
      from {{ref('square_cafe_orders')}} 
      group by 1
    ) tr on tr.location_code = cm.square
  ) cr on cr.micros = m.cafe
where created_at < cr.square_start_date

