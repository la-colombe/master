select

id,
customer_id,
email,
customer_order_number,
financial_status,
fulfillment_status,
processing_method,
gateway,
previous_order_created_at,
customer_created_at,
net_sales,
gross_sales,
weight,
time_since_customer_creation,
time_since_first_order,
time_since_previous_order,
is_most_recent_order,
created_at,
fday as created_at_fday,
fweek as created_at_fweek,
fperiod as created_at_fperiod,
fyear as created_at_fyear,
fquarter as created_at_fquarter,
fday_of_week as created_at_fday_of_week,
fday_of_period as created_at_fday_of_period,
updated_at

from {{ref('shopify_subscription_orders')}} co
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', co.created_at)