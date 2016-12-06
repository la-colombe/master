{{
  config({
    "materialized" : "table",
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
	rc.fday as created_at_fday,
	rc.fweek as created_at_fweek,
	rc.fperiod as created_at_fperiod,
	rc.fyear as created_at_fyear,
	rc.fquarter as created_at_fquarter,
	rc.fday_of_week as created_at_fday_of_week,
	rc.fday_of_period as created_at_fday_of_period,

	forc.fday as first_order_fday,
	forc.fweek as first_order_fweek,
	forc.fperiod as first_order_fperiod,
	forc.fyear as first_order_fyear,
	forc.fquarter as first_order_fquarter,
	forc.fday_of_week as first_order_fday_of_week,
	forc.fday_of_period as first_order_fday_of_period


 from {{ref('shopify_non_subscription_customers')}} c
 join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', c.created_at)
join {{ref('retail_calendar')}} forc on forc.date = date_trunc('day', c.first_order_date)