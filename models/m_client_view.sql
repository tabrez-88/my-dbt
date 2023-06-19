{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        c.encodedkey as external_id,
        s1.id as created_by,
        s2.id as last_modified_by,
        "birthdate" as date_of_birth,
        "creationdate" as submittedon_date,
        /*{{ decode_base64("emailaddress") }} as email_address,*/
        {{ decode_base64("firstname") }} as firstname,
        {{ decode_base64("lastname") }} as lastname,
        (SELECT id FROM m_code_value WHERE code_value = {{ decode_base64("gender") }}) as gender_cv_id,
        "lastmodifieddate" as last_modified_on_utc,
        "lastmodifieddate" as updated_on,
        /*{{ decode_base64("profilepicturekey") }} as image_id,
        {{ decode_base64("profilesignaturekey") }} as signature_id,*/
        "ID" as account_no,
        {{ decode_base64("mobilephone1") }} as mobile_no,
        o.id as office_id,
        CASE 
            WHEN {{ decode_base64("STATE") }} = 'ACTIVE' THEN 300 
            WHEN {{ decode_base64("STATE") }} = 'EXITED' THEN 600
            WHEN {{ decode_base64("STATE") }} = 'REJECTED' THEN 700
            WHEN {{ decode_base64("STATE") }} = 'PENDING_APPROVAL' THEN 100
            WHEN {{ decode_base64("STATE") }} = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        "activationdate" as activation_date,
        "closeddate" as closedon_date
    FROM {{ ref('final_client') }} c
    LEFT JOIN m_staff_view s1 ON s1.external_id = c.assigneduserkey
    LEFT JOIN m_staff_view s2 ON s2.external_id = c.assigneduserkey
    LEFT JOIN m_office_view o ON o.external_id = c.assignedbranchkey
)
SELECT * FROM base
