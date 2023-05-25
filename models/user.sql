{{ config(materialized='table') }}

SELECT *
FROM fineract_default.public."user";
