{{
    config(
        materialized = 'table'
    )
}}

WITH final_client AS (
    SELECT 
        encodedkey,
        {{ decode_base64("firstname") }} AS firstname,
        {{ decode_base64("lastname") }} AS lastname,
        assigneduserkey,
        assignedbranchkey,
        birthdate,
        creationdate,
        mobilephone1,
        ID,
        lastmodifieddate,
        activationdate,
        closeddate
    FROM {{ ref('m_client_view') }}
),
base AS (
    SELECT 
        ROW_NUMBER() OVER () AS id,
        c.encodedkey AS external_id,
        s1.id AS created_by,
        s1.id AS last_modified_by,
        c.birthdate AS date_of_birth,
        c.creationdate AS submittedon_date,
        /*{{ decode_base64("emailaddress") }} AS email_address,*/
        c.firstname,
        c.lastname,
        (SELECT id FROM m_code_value WHERE code_value = {{ decode_base64("gender") }}) AS gender_cv_id,
        c.lastmodifieddate AS last_modified_on_utc,
        c.lastmodifieddate AS updated_on,
        /*{{ decode_base64("profilepicturekey") }} AS image_id,
        {{ decode_base64("profilesignaturekey") }} AS signature_id,*/
        c.ID AS account_no,
        c.mobilephone1 AS mobile_no,
        o.id AS office_id,
        CASE 
            WHEN {{ decode_base64("STATE") }} = 'ACTIVE' THEN 300 
            WHEN {{ decode_base64("STATE") }} = 'EXITED' THEN 600
            WHEN {{ decode_base64("STATE") }} = 'REJECTED' THEN 700
            WHEN {{ decode_base64("STATE") }} = 'PENDING_APPROVAL' THEN 100
            WHEN {{ decode_base64("STATE") }} = 'BLACKLISTED' THEN 400
            ELSE 0 
        END AS status_enum,
        c.activationdate AS activation_date,
        c.closeddate AS closedon_date
    FROM final_client c
    LEFT JOIN m_staff_view s1 ON s1.external_id = c.assigneduserkey
    LEFT JOIN m_office_view o ON o.external_id = c.assignedbranchkey
)
SELECT * FROM base
