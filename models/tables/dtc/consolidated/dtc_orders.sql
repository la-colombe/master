{{
  config({
    "materialized" : "table",
    "unique_key" : "id",
    "sort" : "created_at",
    })
}}

select

o.id,
o.order_number,
o.customer_id,
o.email,
o.shipping_city,
o.shipping_country,
o.shipping_country_code,
o.shipping_latitude,
o.shipping_longitude,
o.shipping_state,
o.shipping_state_code,
o.shipping_zip,
o.financial_status,
o.fulfillment_status,
o.processing_method,
o.gateway,
o.created_at,
date_trunc('day', o.created_at) as created_at_date,
o.customer_created_at,
o.time_since_customer_creation,
o.shipping_price,

--Combined
o.customer_order_number,
o.previous_order_created_at,
o.net_sales,
o.gross_sales,
o.weight,
o.time_since_first_order,
o.time_since_previous_order,
o.is_most_recent_order,

--Sub
so.customer_order_number as subscription_customer_order_number,
so.previous_order_created_at as subscription_previous_order_created_at,
so.net_sales as subscription_net_sales,
so.gross_sales as subscription_gross_sales,
so.weight as subscription_weight,
so.time_since_first_order as subscription_time_since_first_order,
so.time_since_previous_order as subscription_time_since_previous_order,
so.is_most_recent_order as subscription_is_most_recent_order,

--Non-Sub
nso.customer_order_number as non_subscription_customer_order_number,
nso.previous_order_created_at as non_subscription_previous_order_created_at,
nso.net_sales as non_subscription_net_sales,
nso.gross_sales as non_subscription_gross_sales,
nso.weight as non_subscription_weight,
nso.time_since_first_order as non_subscription_time_since_first_order,
nso.time_since_previous_order as non_subscription_time_since_previous_order,
nso.is_most_recent_order as non_subscription_is_most_recent_order,

--Shipstation
sa.total_cost as shipping_cost,
sa.shipments as number_of_shipments

from {{ref('shopify_orders')}} o
left join {{ref('shopify_subscription_orders')}} so on so.id = o.id
left join {{ref('shopify_non_subscription_orders')}} nso on nso.id = o.id
left join {{ref('shipstation_aggregate')}} sa on sa.order_number = o.order_number
