{{
  config({
    "materialized" : "table",
    "sort" : "transaction_date",
    "unique_key" : "unique_invoice_id"
    })
}}


SELECT 
  i.unique_invoice_id,
  i.transaction_date, 
  i.customer_code,
  --i.header_number,
  --i.invoice_number,
  --i.invoice_type,
  i.time_since_previous_paid_coffee_invoice,
  i.invoice,
  i.ship_date,
  i.ship_to_name,
  i.ship_to_address1,
  i.ship_to_address2,
  i.ship_to_address3,
  i.ship_to_city,
  i.ship_to_state,
  i.ship_to_zip,
  i.ship_to_country,
  i.invoice_sales_tax,
  i.invoice_freight,
  i.comment,
  
  i.bill_to_name as account_name,
  --i.account_division,
  --i.company_code,
  --i.account_min_vol,
  --i.account_tier,
  --i.sales_rep_name,
  --i.primary_account_manager_name,
  --i.secondary_account_manager_name,
  --i.account_invoice_number,
  --i.account_total_invoices,
  --i.account_total_paid_coffee_invoices,
  --i.account_second_paid_coffee_invoice_date,
  --i.account_total_quantity,
  --i.account_total_extension,
  
  i.total_quantity,
  i.total_quantity_ordered,
  i.total_quantity_backordered,
  i.total_extension,
  i.total_weight,
  i.account_paid_coffee_invoice_number

from {{ref('warehouse_invoices')}} i
left join {{ref('wholesale_accounts')}} a on a.customer_code = i.customer_code