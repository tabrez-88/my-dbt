{{ config(materialized='table') }}

SELECT *
FROM "public"."m_savings_account"