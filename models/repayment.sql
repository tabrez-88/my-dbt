{{ config(materialized='table') }}

SELECT *
FROM "public"."repayment"