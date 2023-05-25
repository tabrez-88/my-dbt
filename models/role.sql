{{ config(materialized='table') }}

SELECT *
FROM "public"."role"
