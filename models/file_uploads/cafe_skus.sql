{{
  config({
    "materialized" : "table",
    "unique_key" : "square_name, sku"
    })
}}


select square_name as square, sku, product_name, default_weekly_volume, ratio, date_trunc('day', start_date) as start_date
from google_sheets.cafe_skus 