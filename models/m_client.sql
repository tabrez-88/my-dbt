{{ config(materialized='table') }}

WITH final_client_mapping AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        fc.ENCODEDKEY as external_id,
        ms.id as created_by,
        ms.id as last_modified_by,
        fc.BIRTHDATE as date_of_birth,
        fc.CREATIONDATE as submittedon_date,
        fc.EMAILADDRESS as email_address,
        fc.FIRSTNAME as firstname,
        fc.MIDDLENAME as middlename,
        fc.LASTNAME as lastname,
        cv.id as gender_cv_id,
        fc.LASTMODIFIEDDATE as last_modified_on_utc,
        fc.LASTMODIFIEDDATE as updated_on,
        fc.ID as account_no,
        fc.MOBILEPHONE1 as mobile_no,
        mo.id as office_id,
        CASE fc.STATE
            WHEN 'ACTIVE' THEN 300
            WHEN 'EXITED' THEN 600
            WHEN 'REJECTED' THEN 700
            WHEN 'PENDING_APPROVAL' THEN 100
            WHEN 'BLACKLISTED' THEN 500
            ELSE 0
        END as status_enum,
        fc.ACTIVATIONDATE as activation_date,
        fc.CLOSEDDATE as closedon_date
    FROM {{ ref('final_client') }} AS fc
    LEFT JOIN {{ ref('m_staff') }} AS ms ON fc.ASSIGNEDUSERKEY = ms.external_id
    LEFT JOIN {{ ref('m_code_value') }} AS cv ON fc.GENDER = cv.code_id
    LEFT JOIN {{ ref('m_office') }} AS mo ON fc.ASSIGNEDBRANCHKEY = mo.external_id
)

SELECT * FROM final_client_mapping
