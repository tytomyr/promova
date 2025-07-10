   {{
     config(
       materialized='table'
     )
   }}

   {{ dbt_date.get_date_dimension(
       start_date="2015-01-01",
       end_date="2025-12-31"
      )
   }}