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

  case
    when i.customer_code in (select warehouse from analytics.cafe_mapping) then 'Cafe'
    when left(i.customer_code,2)  = '01' then 'Philly'
    when left(i.customer_code,2)  = '15' then 'CPG'
    when left(i.customer_code,2)  = '20' or left(i.customer_code,2)  = '40' or left(i.customer_code,2)  = '55' then 'New York'
    when left(i.customer_code,2)  = '30' then 'DC'
    when left(i.customer_code,2)  = '50' then 'Chicago'
    when left(i.customer_code,2)  = '60' or left(i.customer_code,2)  = '25' then 'West Coast'
    when left(i.customer_code,2)  = '61' then 'California'
    when left(i.customer_code,2)  = '64' then 'Florida'
    when left(i.customer_code,2)  = '65' then 'Boston'
    when left(i.customer_code,2)  = '70' then 'Nationals'
    else 'Other'
  end as region,

  case
    when i.customer_code like '15%' then 'CPG'
    when i.customer_code in (select warehouse from analytics.cafe_mapping) then 'Internal'
    when i.customer_code like '%LCTX%' or i.customer_code like '%LACX%' then 'Internal'
    else 'Hospitality'
  end as customer_type,

  case 
    when substring(i.customer_code, 5, 1) = 'C' then 'Cafe'
    when substring(i.customer_code, 5, 1) = 'D' then 'Distributor'
    when substring(i.customer_code, 5, 1) = 'S' then 'Small Market/Retail'
    when substring(i.customer_code, 5, 1) = 'R' then 'Restaurant'
    when substring(i.customer_code, 5, 1) = 'P' then 'People'
    when substring(i.customer_code, 5, 1) = 'O' then 'Office'
    when substring(i.customer_code, 5, 1) = 'H' then 'Hotels'
    when substring(i.customer_code, 5, 1) = 'G' then 'Groups'
    when substring(i.customer_code, 5, 1) = 'F' then 'Catering/Bakery'
  end as business_type,

  fday as transaction_date_fday,
  fweek as transaction_date_fweek,
  fperiod as transaction_date_fperiod,
  fyear as transaction_date_fyear,
  fquarter as transaction_date_fquarter,
  fday_of_week as transaction_date_fday_of_week,
  fday_of_period as transaction_date_fday_of_period,
  is_last_week,
  is_last_month,
  is_ty,
  is_ly,
  is_wtd,
  is_mtd,
  is_qtd,
  is_ytd

from {{ref('warehouse_invoices')}} i
left join {{ref('wholesale_accounts')}} a on a.customer_code = i.customer_code
left join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', i.transaction_date)