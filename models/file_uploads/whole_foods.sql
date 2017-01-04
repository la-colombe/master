{{
  config({
    "materialized" : "table",
    "unique_key" : "unique_key",
    "sort" : "start_date"
    })
}}


select md5(start_date || store_number || upc) as unique_key, start_date, end_date, region, store_name, store_status, store_number, category, class, sub_category, lpad(upc, 12, '0') as upc, name, volume, units, net_sales, gross_sales, quantity
from google_sheets.whole_foods

union 

select md5(start_date || store_number || upc) as unique_key, start_date, end_date, region, store_name, store_status, store_number, category, class, sub_category, lpad(upc, 12, '0') as upc, name, volume, units, net_sales, gross_sales, quantity
from google_sheets.whole_foods_2014

union 

select md5(start_date || store_number || upc) as unique_key, start_date, end_date, region, store_name, store_status, store_number, category, class, sub_category, lpad(upc, 12, '0') as upc, name, volume, units, net_sales, gross_sales, quantity
from google_sheets.whole_foods_2015

union 

select md5(start_date || store_number || upc) as unique_key, start_date, end_date, region, store_name, store_status, store_number, category, class, sub_category, lpad(upc, 12, '0') as upc, name, volume, units, net_sales, gross_sales, quantity
from google_sheets.whole_foods_2016_1

union 

select md5(start_date || store_number || upc) as unique_key, start_date, end_date, region, store_name, store_status, store_number, category, class, sub_category, lpad(upc, 12, '0') as upc, name, volume, units, net_sales, gross_sales, quantity
from google_sheets.whole_foods_2016_2