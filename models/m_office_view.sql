WITH source AS (
    SELECT
        {{ decode_base64("encodedkey")}} as external_id,
        "ID" as id,
        {{ decode_base64("NAME") }} AS name,
        CREATIONDATE as opening_date
    FROM branch
)

SELECT
    id::int8 as id,
    CAST(NULL AS int8) as parent_id,
    NULL as hierarchy,
    external_id,
    name,
    opening_date
FROM source