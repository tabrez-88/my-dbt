{{ config(materialized='table') }}

SELECT 
    ROW_NUMBER() OVER () as id,
    r."NAME" as name,
    'No description available' as description,
    FALSE as is_disabled
FROM {{ ref('role') }} AS r 