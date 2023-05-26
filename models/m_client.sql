{{ config(materialized='table') }}

WITH final_client_mapping AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        {{ decode_base64(fc.ENCODEDKEY) }} as external_id,
        ms.id as created_by,
        ms.id as last_modified_by,
        fc.BIRTHDATE as date_of_birth,
        fc.CREATIONDATE as submittedon_date,
        {{ decode_base64(fc.EMAILADDRESS) }} as email_address,
        {{ decode_base64(fc.FIRSTNAME) }} as firstname,
        {{ decode_base64(fc.MIDDLENAME) }} as middlename,
        {{ decode_base64(fc.LASTNAME) }} as lastname,
        cv.id as gender_cv_id,
        fc.LASTMODIFIEDDATE as last_modified_on_utc,
        fc.LASTMODIFIEDDATE as updated_on,
        {{ decode_base64(fc.ID) }} as account_no,
        {{ decode_base64(fc.MOBILEPHONE1) }} as mobile_no,
        mo.id as office_id,
        CASE 
            WHEN fc.STATE = 'ACTIVE' THEN 300
            WHEN fc.STATE = 'EXITED' THEN 600
            WHEN fc.STATE = 'REJECTED' THEN 700
            WHEN fc.STATE = 'PENDING_APPROVAL' THEN 100
            WHEN fc.STATE = 'BLACKLISTED' THEN 500
            ELSE 0
        END as status_enum,
        fc.ACTIVATIONDATE as activation_date,
        fc.CLOSEDDATE as closedon_date
    FROM {{ ref('final_client') }} AS fc
    LEFT JOIN {{ ref('m_staff') }} AS ms ON {{ decode_base64(fc.ASSIGNEDUSERKEY) }} = ms.external_id
    LEFT JOIN {{ ref('m_code_value') }} AS cv ON {{ decode_base64(fc.GENDER) }} = cv.code_value
    LEFT JOIN {{ ref('m_office') }} AS mo ON {{ decode_base64(fc.ASSIGNEDBRANCHKEY) }} = mo.external_id
)

SELECT * FROM final_client_mapping;
