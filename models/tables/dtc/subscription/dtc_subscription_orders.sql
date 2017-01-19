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
created_at

from {{ref('shopify_subscription_orders')}} co