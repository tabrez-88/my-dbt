{{ config(materialized='table') }}

WITH decoded_client AS (
    SELECT 
        fcl.BIRTHDATE as date_of_birth,
        fcl.CREATIONDATE as submittedon_date,
        fcl.LASTMODIFIEDDATE as last_modified_on_utc,
        fcl.ACTIVATIONDATE as activation_date,
        fcl.CLOSEDDATE as closedon_date,
        {{ decode_base64('fcl.ENCODEDKEY') }} as external_id,
        {{ decode_base64('fcl.EMAILADDRESS') }} as email_address,
        {{ decode_base64('fcl.FIRSTNAME') }} as firstname,
        fcl.MIDDLENAME as middlename,
        {{ decode_base64('fcl.LASTNAME') }} as lastname,
        {{ decode_base64('fcl.MOBILEPHONE1') }} as mobile_no,
        {{ decode_base64('fcl.ASSIGNEDUSERKEY') }} as assigned_user_key,
        {{ decode_base64('fcl.ASSIGNEDBRANCHKEY') }} as assigned_branch_key,
        {{ decode_base64('fcl."gender"') }} as gender,
        {{ decode_base64('fcl."STATE"') }} as state
    FROM {{ ref('final_client') }} AS fcl
),
final_client_mapping AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        dc.external_id,
        m_staff.id as created_by,
        m_staff.id as last_modified_by,
        dc.date_of_birth,
        dc.submittedon_date,
        dc.email_address,
        dc.firstname,
        dc.middlename,
        dc.lastname,
        (SELECT id FROM m_code_value WHERE code_value = dc.gender)  as gender_cv_id,
        dc.last_modified_on_utc,
        dc.last_modified_on_utc as updated_on,
        dc.mobile_no,
        m_office.id as office_id,
        CASE 
            WHEN dc.state = 'ACTIVE' THEN 300
            WHEN dc.state = 'EXITED' THEN 600
            WHEN dc.state = 'REJECTED' THEN 700
            WHEN dc.state = 'PENDING_APPROVAL' THEN 100
            WHEN dc.state = 'BLACKLISTED' THEN 500
            ELSE 0
        END as status_enum,
        dc.activation_date,
        dc.closedon_date
    FROM decoded_client AS dc
    LEFT JOIN {{ ref('m_staff') }} AS m_staff ON dc.assigned_user_key = m_staff.external_id
    LEFT JOIN {{ ref('m_office') }} AS m_office ON dc.assigned_branch_key = m_office.external_id
)

SELECT * FROM final_client_mapping
