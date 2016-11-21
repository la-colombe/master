{{
  config({
    "materialized" : "incremental",
    "sql_where" : "updated_at > (select max(updated_at) from {{this}})",
    "unique_key" : "id",
    "sort" : "created_at",
    })
}}

select
	id,
	first_name,
	last_name,
	email,
	state,
	tags,
	tax_exempt,
	updated_at,
	number_of_orders,
	average_order_value,
	first_order_date,
	last_order_date,
	years_active,
	lifetime_revenue,
	items_purchased,
	annual_revenue,
	items_per_order,

	created_at,
	fday as created_at_fday,
	fweek as created_at_fweek,
	fperiod as created_at_fperiod,
	fyear as created_at_fyear,
	fday_of_week as created_at_fday_of_week,
	fday_of_period as created_at_fday_of_period


 from {{ref('shopify_non_subscription_customers')}} c
 join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', c.created_at)