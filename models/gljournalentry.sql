{{ config(materialized='table') }}

SELECT *
FROM "public"."gljournalentry"