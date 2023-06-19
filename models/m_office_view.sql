WITH source AS (
    SELECT
        convert_from(decode("encodedkey", 'base64'), 'UTF8') as external_id,
        "ID" as id,
        convert_from(decode("NAME", 'base64'), 'UTF8') AS name,
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
FROM source;
