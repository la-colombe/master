{{
  config({
    "materialized" : "table",
    "unique_key" : "id",
    "sort" : "created_at",
    })
}}

select 

oi.id,
oi.order_id,
oi.product_id,
oi.variant_id,
oi.customer_id,
oi.name,
oi.title,
oi.variant_title,
oi.product_type,
oi.sku,
oi.price,
oi.quantity,
oi.line_item_discount,
oi.line_item_net_sales,
oi.line_item_gross_sales,
oi.line_item_weight,
oi.weight,
oi.weight_unit,
oi.financial_status,
oi.fulfillment_status,
oi.subscription_type,
oi.updated_at,
oi.created_at,
o.created_at_fday,
o.created_at_fweek,
o.created_at_fperiod,
o.created_at_fyear,
o.created_at_fquarter,
o.created_at_fday_of_week,
o.created_at_fday_of_period,

--Consolidated
oi.customer_order_number,
oi.order_net_sales,
oi.order_gross_sales,
oi.order_weight,

--Sub
soi.customer_order_number as subscription_customer_order_number,
soi.order_net_sales as subscription_order_net_sales,
soi.order_gross_sales as subscription_order_gross_sales,
soi.order_weight as subscription_order_weight,

--Non-Sub
nsoi.customer_order_number as non_subscription_customer_order_number,
nsoi.order_net_sales as non_subscription_order_net_sales,
nsoi.order_gross_sales as non_subscription_order_gross_sales,
nsoi.order_weight as non_subscription_order_weight

from {{ref('shopify_order_items')}} oi
join {{ref('dtc_orders')}} o on o.id = oi.order_id
left join {{ref('shopify_subscription_order_items')}} soi on soi.id = oi.id
left join {{ref('shopify_non_subscription_order_items')}} nsoi on nsoi.id = oi.id