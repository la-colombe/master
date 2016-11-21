{{
  config({
    "materialized" : "incremental",
    "sql_where" : "updated_at > (select max(updated_at) from {{this}})",
    "unique_key" : "unique_id",
    "sort" : "created_at",
    "dist" : 'location_code'
    })
}}

select
	unique_id,
	convert_timezone(timezone, created_at) as created_at,
	gross_sales,
	net_sales,
	discounts,
	updated_at,

--Location
	open_date,
	comp_date,
	name,
	region,
	store_type,
	location_code,

--Retail Calendar
	fday as created_at_fday,
	fweek as created_at_fweek,
	fperiod as created_at_fperiod,
	fyear as created_at_fyear,
	fday_of_week as created_at_fday_of_week,
	fday_of_period as created_at_fday_of_period
      
from {{ref('square_cafe_order_items')}} o 
join {{ref('cafe_mapping')}} cm on o.location_code = cm.square
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', convert_timezone(timezone, created_at))