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
oi.created_at,

--Consolidated
oi.customer_order_number,
oi.order_net_sales,
oi.order_gross_sales,
oi.order_weight

from {{ref('shopify_subscription_order_items')}} oi
join {{ref('dtc_subscription_orders')}} o on o.id = oi.order_id