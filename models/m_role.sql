{{ config(materialized='table') }}

SELECT 
    ROW_NUMBER() OVER () as id,
    {{ decode_base64("r"."NAME") }} as name,
    'No description available' as description,
    FALSE as is_disabled
FROM {{ ref('role') }} AS r 