{{ config(materialized='table') }}

SELECT *
FROM "public"."m_code_value"
