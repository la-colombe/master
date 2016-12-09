{{
  config({
    "materialized" : "table",
    "unique_key" : "id",
    "sort" : "created_at",
    })
}}

select

o.id,
o.customer_id,
o.email,
o.financial_status,
o.fulfillment_status,
o.processing_method,
o.gateway,
o.created_at,
rc.fday as created_at_fday,
rc.fweek as created_at_fweek,
rc.fperiod as created_at_fperiod,
rc.fyear as created_at_fyear,
rc.fquarter as created_at_fquarter,
rc.fday_of_week as created_at_fday_of_week,
rc.fday_of_period as created_at_fday_of_period,
is_last_week,
is_last_month,
is_ty,
is_ly,
is_wtd,
is_mtd,
is_ytd,
o.updated_at,
o.customer_created_at,
o.time_since_customer_creation,

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
nso.is_most_recent_order as non_subscription_is_most_recent_order

from {{ref('shopify_orders')}} o
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', o.created_at)
left join {{ref('shopify_subscription_orders')}} so on so.id = o.id
left join {{ref('shopify_non_subscription_orders')}} nso on nso.id = o.id
