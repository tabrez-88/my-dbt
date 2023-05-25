{{ config(materialized='table') }}

SELECT 
    {{ decode_base64(r."ID") }}  as id,
    r."NAME" as name,
    'No description available' as description,
    FALSE as is_disabled
FROM {{ ref('role') }} AS r
