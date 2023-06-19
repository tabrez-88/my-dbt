{{
    config(
        materialized = 'table'
    )
}}

WITH final_client AS (
    SELECT 
        {{ decode_base64("encodedkey") }} AS encodedkey,
        {{ decode_base64("assigneduserkey") }} AS assigneduserkey,
        {{ decode_base64("assignedbranchkey") }} AS assignedbranchkey,
        {{ decode_base64("firstname") }} AS firstname,
        {{ decode_base64("lastname") }} AS lastname,
        {{ decode_base64("STATE") }} AS state,
        {{ decode_base64("birthdate") }} AS birthdate,
        {{ decode_base64("creationdate") }} AS creationdate,
        {{ decode_base64("mobilephone1") }} AS mobilephone1,
        {{ decode_base64("ID") }} AS ID,
        {{ decode_base64("lastmodifieddate") }} AS lastmodifieddate,
        {{ decode_base64("activationdate") }} AS activationdate,
        {{ decode_base64("closeddate") }} AS closeddate
    FROM {{ ref('m_client_view') }}
),
base AS (
    SELECT 
        ROW_NUMBER() OVER () AS id,
        encodedkey AS external_id,
        s1.id AS created_by,
        s2.id AS last_modified_by,
        birthdate AS date_of_birth,
        creationdate AS submittedon_date,
        /*{{ decode_base64("emailaddress") }} AS email_address,*/
        firstname,
        lastname,
        (SELECT id FROM m_code_value WHERE code_value = {{ decode_base64("gender_cv_id") }}) AS gender_cv_id,
        lastmodifieddate AS last_modified_on_utc,
        lastmodifieddate AS updated_on,
        /*{{ decode_base64("profilepicturekey") }} AS image_id,
        {{ decode_base64("profilesignaturekey") }} AS signature_id,*/
        ID AS account_no,
        mobilephone1 AS mobile_no,
        o.id AS office_id,
        CASE 
            WHEN state = 'ACTIVE' THEN 300 
            WHEN state = 'EXITED' THEN 600
            WHEN state = 'REJECTED' THEN 700
            WHEN state = 'PENDING_APPROVAL' THEN 100
            WHEN state = 'BLACKLISTED' THEN 400
            ELSE 0 
        END AS status_enum,
        activationdate AS activation_date,
        closeddate AS closedon_date
    FROM final_client c
    LEFT JOIN m_staff_view s1 ON s1.external_id = c.assigneduserkey
    LEFT JOIN m_staff_view s2 ON s2.external_id = c.assigneduserkey
    LEFT JOIN m_office_view o ON o.external_id = c.assignedbranchkey
)
SELECT * FROM base
