{{ config(materialized='table') }}

SELECT *
FROM "public"."m_saving_account"