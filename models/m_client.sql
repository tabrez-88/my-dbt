{{ config(materialized='table') }}

WITH final_client_mapping AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        decode_base64_or_text(fcl.ENCODEDKEY) as external_id,
        m_staff.id as created_by,
        m_staff.id as last_modified_by,
        fcl.BIRTHDATE as date_of_birth,
        fcl.CREATIONDATE as submittedon_date,
        decode_base64_or_text(fcl.EMAILADDRESS) as email_address,
        decode_base64_or_text(fcl.FIRSTNAME) as firstname,
        decode_base64_or_text(fcl.MIDDLENAME) as middlename,
        decode_base64_or_text(fcl.LASTNAME) as lastname,
       (SELECT id FROM m_code_value WHERE code_value = decode_base64_or_text(fcl."gender"))  as gender_cv_id,
        fcl.LASTMODIFIEDDATE as last_modified_on_utc,
        fcl.LASTMODIFIEDDATE as updated_on,
        decode_base64_or_text(fcl.MOBILEPHONE1) as mobile_no,
        m_office.id as office_id,
        CASE 
            WHEN decode_base64_or_text(fcl."STATE") = 'ACTIVE' THEN 300
            WHEN decode_base64_or_text(fcl."STATE") = 'EXITED' THEN 600
            WHEN decode_base64_or_text(fcl."STATE") = 'REJECTED' THEN 700
            WHEN decode_base64_or_text(fcl."STATE") = 'PENDING_APPROVAL' THEN 100
            WHEN decode_base64_or_text(fcl."STATE") = 'BLACKLISTED' THEN 500
            ELSE 0
        END as status_enum,
        fcl.ACTIVATIONDATE as activation_date,
        fcl.CLOSEDDATE as closedon_date
    FROM {{ ref('final_client') }} AS fcl
    LEFT JOIN {{ ref('m_staff') }} AS m_staff ON decode_base64_or_text(fcl.ASSIGNEDUSERKEY) = m_staff.external_id
    LEFT JOIN {{ ref('m_office') }} AS m_office ON decode_base64_or_text(fcl.ASSIGNEDBRANCHKEY) = m_office.external_id
)

SELECT * FROM final_client_mapping
