{{
  config({
    "materialized" : "table",
    "unique_key" : "id",
    "sort" : "created_at",
    "dist" : 'location_code'
    })
}}
select
	id,
	convert_timezone(timezone, created_at) as created_at,
	gross_sales,
	total_collections,
	total_collections_net_processing,
	net_sales,
	discounts,
	taxes,
	refunds,
	net_refund,
	tips,
	fees,
	units,
	updated_at,

--Order Item Aggregates
	employee_bag,
	expired_bag,

--Location
	open_date,
	comp_date,
	name,
	region,
	store_type,
	location_code,
	is_comp,
--Retail Calendar
	to_char(convert_timezone(timezone, created_at), 'HH24') as created_at_hour,
	fday as created_at_fday,
	fweek as created_at_fweek,
	fperiod as created_at_fperiod,
	fyear as created_at_fyear,
	fquarter as created_at_fquarter,
	fday_of_week as created_at_fday_of_week,
	fday_of_period as created_at_fday_of_period,
	is_last_week,
	is_last_month,
	is_ty,
	is_ly,
	is_wtd,
	is_mtd,
	is_qtd,
	is_ytd
      
from {{ref('square_cafe_orders')}} o 
join {{ref('cafe_mapping')}} cm on o.location_code = cm.square
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', convert_timezone(timezone, created_at))

UNION

select 
	md5(pk) as id,
	m.date as created_at,
	revenue as gross_sales,
	null as total_collections,
	null as total_collections_net_processing,
	round(revenue * (1-rate), 2) as net_sales,
	null as discounts,
	round(revenue * rate, 2) as taxes,
	null as refunds,
	null as net_refund,
	null as tips,
	null as fees,
	count as units,
	m.date as updated_at,

	--Order Item Aggregates
	NULL as employee_bag,
	NULL as expired_bag,

--Location
	open_date,
	comp_date,
	cr.name,
	region,
	store_type,
	location_code,
	is_comp,

--Retail Calendar
	NULL as created_at_hour,
	fday as created_at_fday,
	fweek as created_at_fweek,
	fperiod as created_at_fperiod,
	fyear as created_at_fyear,
	fquarter as created_at_fquarter,
	fday_of_week as created_at_fday_of_week,
	fday_of_period as created_at_fday_of_period,
	is_last_week,
	is_last_month,
	is_ty,
	is_ly,
	is_wtd,
	is_mtd,
	is_qtd,
	is_ytd

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
join {{ref('retail_calendar')}} rc on rc.date = m.date
where created_at < cr.square_start_date
