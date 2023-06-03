{{ config(materialized='table') }}

SELECT *
FROM "public"."loanproduct"