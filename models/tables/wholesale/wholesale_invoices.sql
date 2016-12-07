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
  i.bill_to_name,
  i.header_number,
  i.invoice_number,
  i.invoice_type,
  i.invoice,
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
  
  i.account_name,
  i.account_division,
  i.company_code,
  i.account_min_vol,
  i.account_tier,
  i.sales_rep_name,
  i.primary_account_manager_name,
  i.secondary_account_manager_name,
  i.account_invoice_number,
  i.account_total_invoices,
  i.account_total_paid_coffee_invoices,
  i.account_second_paid_coffee_invoice_date,
  i.account_total_quantity,
  i.account_total_extension,
  
  i.total_quantity,
  i.total_quantity_ordered,
  i.total_quantity_backordered,
  i.total_extension,
  i.total_weight,
  i.account_paid_coffee_invoice_number,

  a.region,
  a.customer_type,
  a.business_type,

  fday as transaction_date_fday,
  fweek as transaction_date_fweek,
  fperiod as transaction_date_fperiod,
  fyear as transaction_date_fyear,
  fquarter as transaction_date_fquarter,
  fday_of_week as transaction_date_fday_of_week,
  fday_of_period as transaction_date_fday_of_period

from {{ref('warehouse_invoices')}} i
left join {{ref('wholesale_accounts')}} a on a.customer_code = i.customer_code
left join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', i.transaction_date)