{{
    config(
        materialized='table'
    )
}}

SELECT
    row_number() OVER (ORDER BY encodedkey) as id,
    {{ decode_base64("ID") }} as code_id,
    {{ decode_base64("NAME") }} as code_value,
    {{ decode_base64("encodedkey")}} as code_description,
    0 as order_position,
    NULL as code_score,
    true as is_active,
    false as is_mandatory
FROM grouprolename
