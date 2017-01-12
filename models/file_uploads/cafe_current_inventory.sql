{{
  config({
    "materialized" : "table",
    "unique_key" : "square, sku"
    })
}}

select square, sku, inventory_date, inventory
from google_sheets.cafe_current_inventory 