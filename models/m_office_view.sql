WITH source AS (
    SELECT
        ENCODEDKEY as external_id,
        "ID" as id,
        {{ decode_base64("NAME") }} AS name,
        CREATIONDATE as opening_date
    FROM final_branch
)

SELECT
    id::int8 as id,
    cast(NULL as int8) as parent_id,
    NULL as hierarchy,
    external_id,
    name,
    opening_date
FROM source