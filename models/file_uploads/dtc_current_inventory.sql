{{
  config({
    "materialized" : "table",
    "unique_key" : "sku"
    })
}}

select sku, inventory_date, inventory
from google_sheets.dtc_current_inventory 