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
customer_id,
email,
customer_order_number,
financial_status,
fulfillment_status,
processing_method,
gateway,
previous_order_created_at,
customer_created_at,
subtotal_price,
total_weight,
time_since_customer_creation,
time_since_first_order,
time_since_previous_order,
is_most_recent_order,
created_at,
fday as created_at_fday,
fweek as created_at_fweek,
fperiod as created_at_fperiod,
fyear as created_at_fyear,
fday_of_week as created_at_fday_of_week,
fday_of_period as created_at_fday_of_period,
updated_at

from {{ref('shopify_subscription_orders')}} co
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', co.created_at)