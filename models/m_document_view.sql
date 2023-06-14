WITH decoded_document AS (
    SELECT
        {{ decode_base64("encodedkey") }} AS encodedkey,
        {{ decode_base64("LOCATION") }} AS "LOCATION",
        filesize,
        creationdate,
        lastmodifieddate,
        {{ decode_base64("NAME") }} AS "NAME",
        documentholderkey,
        createdbyuserkey,
        {{ decode_base64("originalfilename") }} AS originalfilename,
        {{ decode_base64("description") }} AS description,
        documentholdertype,
        "ID",
        {{ decode_base64("TYPE") }} AS "TYPE"
    FROM {{ ref('document') }}
)
SELECT
    "ID" AS id,
    parent_entity_id,
    "NAME" AS name,
    "originalfilename" AS file_name,
    filesize AS size,
    "TYPE" AS type,
    "description" AS description,
    "LOCATION" AS location,
    NULL AS storage_type_enum
FROM decoded_document
