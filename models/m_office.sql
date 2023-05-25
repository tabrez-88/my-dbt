WITH source AS (
    SELECT
        ENCODEDKEY as external_id,
        "ID" as id,
        {{ decode_base64("NAME") }} AS name,
        CREATIONDATE as opening_date
    FROM branch
)

SELECT
    id::int8 as id,
    NULL as parent_id,
    NULL as hierarchy,
    external_id,
    name,
    opening_date
FROM source