{{ config(materialized='table') }}

SELECT *
FROM fineract_default."user"
