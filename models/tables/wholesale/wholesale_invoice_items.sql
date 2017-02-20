{{
  config({
    "materialized" : "table",
    "sort" : "transaction_date",
    "unique_key" : "unique_invoice_item_id"
    })
}}


SELECT 
  ii.unique_invoice_item_id,
  ii.unique_invoice_id,
  ii.invoice_number,
  ii.sku,
  ii.item_name,
  ii.item_type,
  ii.quantity,
  ii.quantity_ordered,
  ii.quantity_backordered,
  ii.unit_price,
  ii.extension,
  ii.unique_sales_order_id,
  ii.sales_order_number,
  ii.warehouse_code,
  ii.transaction_date, 
  ii.customer_code,
  ii.bill_to_name,
  ii.invoice_type,
  ii.invoice,
  ii.ship_date,
  ii.ship_to_name,
  ii.ship_to_address1,
  ii.ship_to_address2,
  ii.ship_to_address3,
  ii.ship_to_city,
  ii.ship_to_state,
  ii.ship_to_zip,
  ii.ship_to_country,
  ii.invoice_sales_tax,
  ii.invoice_freight,
  ii.comment,
  ii.account_paid_coffee_invoice_number,
  ii.total_weight,
  ii.bill_to_name as account_name,
  ii.account_invoice_number,
  case
    when ii.sku = 'PPPURB50XX' or ii.sku like 'PPPURD%' then 'On Tap'
    when ii.sku like 'C%' then 'Coffee'
    when ii.sku like 'M%' then 'Machines'
    when ii.sku like 'P%' then 'RTD'
    else 'Other'
  end as product_category

  from {{ref('warehouse_invoice_items')}} ii
  left join {{ref('wholesale_accounts')}} a on a.customer_code = ii.customer_code