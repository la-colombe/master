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

	created_at

 from {{ref('shopify_subscription_customers')}} c
