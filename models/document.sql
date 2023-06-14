{{ config(materialized='table') }}

SELECT *
FROM "public"."document"