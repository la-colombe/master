{{
  config({
    "materialized" : "table",
    "unique_key" : "id",
    "sort" : "created_at",
    })
}}

select
--Combined
	c.id,
	c.first_name,
	c.last_name,
	c.email,
	c.state,
	c.tags,
	c.tax_exempt,
	c.updated_at,
	c.created_at,
	rc.fday as created_at_fday,
	rc.fweek as created_at_fweek,
	rc.fperiod as created_at_fperiod,
	rc.fyear as created_at_fyear,
	rc.fquarter as created_at_fquarter,
	rc.fday_of_week as created_at_fday_of_week,
	rc.fday_of_period as created_at_fday_of_period,

--Combined
	c.number_of_orders,
	c.average_order_value,
	c.first_order_date,
	forc.fday as first_order_fday,
	forc.fweek as first_order_fweek,
	forc.fperiod as first_order_fperiod,
	forc.fyear as first_order_fyear,
	forc.fquarter as first_order_fquarter,
	forc.fday_of_week as first_order_fday_of_week,
	forc.fday_of_period as first_order_fday_of_period,
	c.last_order_date,
	c.years_active,
	c.lifetime_revenue,
	c.items_purchased,
	c.annual_revenue,
	c.items_per_order,
--Non Sub
	nsc.number_of_orders as non_subscription_number_of_orders,
	nsc.average_order_value as non_subscription_average_order_value,
	nsc.first_order_date as non_subscription_first_order_date,
	nsc.last_order_date as non_subscription_last_order_date,
	nsc.years_active as non_subscription_years_active,
	nsc.lifetime_revenue as non_subscription_lifetime_revenue,
	nsc.items_purchased as non_subscription_items_purchased,
	nsc.annual_revenue as non_subscription_annual_revenue,
	nsc.items_per_order as non_subscription_items_per_order,
--Sub
	sc.number_of_orders as subscription_number_of_orders,
	sc.average_order_value as subscription_average_order_value,
	sc.first_order_date as subscription_first_order_date,
	sc.last_order_date as subscription_last_order_date,
	sc.years_active as subscription_years_active,
	sc.lifetime_revenue as subscription_lifetime_revenue,
	sc.items_purchased as subscription_items_purchased,
	sc.annual_revenue as subscription_annual_revenue,
	sc.items_per_order as subscription_items_per_order

	from {{ref('shopify_customers')}} c
 	join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', c.created_at)
	left join {{ref('shopify_subscription_customers')}} sc on sc.id = c.id
	left join {{ref('shopify_non_subscription_customers')}} nsc on nsc.id = c.id
	join {{ref('retail_calendar')}} forc on forc.date = date_trunc('day', c.first_order_date)