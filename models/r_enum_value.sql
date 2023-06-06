{{ config(materialized='table') }}

SELECT *
FROM "public"."r_enum_value"