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
  ii.header_number,
  ii.line_number,
  ii.warehouse_code,
  ii.transaction_date, 
  ii.customer_code,
  ii.bill_to_name,
  ii.invoice_type,
  ii.invoice,
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
  ii.account_name,
  ii.account_division,
  ii.company_code,
  ii.account_min_vol,
  ii.account_tier,
  ii.sales_rep_name,
  ii.primary_account_manager_name,
  ii.secondary_account_manager_name,
  ii.account_invoice_number,
  ii.account_second_paid_coffee_invoice_date,
  case
    when ii.sku = 'PPPURB50XX' or ii.sku like 'PPPURD%' then 'On Tap'
    when ii.sku like 'C%' then 'Coffee'
    when ii.sku like 'M%' then 'Machines'
    when ii.sku like 'P%' then 'RTD'
    else 'Other'
  end as product_category,

  case
    when ii.customer_code in (select warehouse from analytics.cafe_mapping) then 'Cafe'
    when left(ii.customer_code,2)  = '01' then 'Philly'
    when left(ii.customer_code,2)  = '15' then 'CPG'
    when left(ii.customer_code,2)  = '20' or left(ii.customer_code,2)  = '40' or left(ii.customer_code,2)  = '55' then 'New York'
    when left(ii.customer_code,2)  = '30' then 'DC'
    when left(ii.customer_code,2)  = '50' then 'Chicago'
    when left(ii.customer_code,2)  = '60' or left(ii.customer_code,2)  = '25' then 'West Coast'
    when left(ii.customer_code,2)  = '61' then 'California'
    when left(ii.customer_code,2)  = '64' then 'Florida'
    when left(ii.customer_code,2)  = '65' then 'Boston'
    when left(ii.customer_code,2)  = '70' then 'Nationals'
    else 'Other'
  end as region,

  case
    when ii.customer_code like '15%' then 'CPG'
    when ii.customer_code in (select warehouse from analytics.cafe_mapping) then 'Internal'
    when ii.customer_code like '%LCTX%' or ii.customer_code like '%LACX%' then 'Internal'
    else 'Hospitality'
  end as customer_type,

  case 
    when substring(ii.customer_code, 5, 1) = 'C' then 'Cafe'
    when substring(ii.customer_code, 5, 1) = 'D' then 'Distributor'
    when substring(ii.customer_code, 5, 1) = 'S' then 'Small Market/Retail'
    when substring(ii.customer_code, 5, 1) = 'R' then 'Restaurant'
    when substring(ii.customer_code, 5, 1) = 'P' then 'People'
    when substring(ii.customer_code, 5, 1) = 'O' then 'Office'
    when substring(ii.customer_code, 5, 1) = 'H' then 'Hotels'
    when substring(ii.customer_code, 5, 1) = 'G' then 'Groups'
    when substring(ii.customer_code, 5, 1) = 'F' then 'Catering/Bakery'
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

  from {{ref('warehouse_invoice_items')}} ii
  left join {{ref('wholesale_accounts')}} a on a.customer_code = ii.customer_code
  left join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', ii.transaction_date)