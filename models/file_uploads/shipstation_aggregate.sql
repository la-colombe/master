{{
  config({
    "materialized" : "table",
    "unique_key" : "order_number"
    })
}}


select split_part(order_number, '-',1) as order_number, 
sum(shipping_cost) as total_cost, 
count(order_number) as shipments
from file_uploads.shipstation_orders
group by 1