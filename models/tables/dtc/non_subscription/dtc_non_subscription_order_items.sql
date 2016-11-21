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
order_id,
product_id,
variant_id,
customer_id,
name,
title,
variant_title,
sku,
price,
quantity,
total_discount,
weight,
weight_unit,
order_subtotal,
customer_order_number,
financial_status,
fulfillment_status,
subscription_type,
created_at,
fday as created_at_fday,
fweek as created_at_fweek,
fperiod as created_at_fperiod,
fyear as created_at_fyear,
fday_of_week created_at_fday_of_week,
fday_of_period created_at_fday_of_period,
updated_at


from {{ref('shopify_non_subscription_order_items')}} coi
join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', coi.created_at)