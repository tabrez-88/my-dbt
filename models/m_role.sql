{{ config(materialized='table') }}

WITH role_decoded AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        {{ decode_base64('NAME') }} as name,
        'No description available' as description,
        FALSE as is_disabled
    FROM {{ ref('role') }} 
)

SELECT * FROM role_decoded
