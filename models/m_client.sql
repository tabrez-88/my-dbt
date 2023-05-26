{{ config(materialized='table') }}

WITH final_client_mapping AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        {{ decode_base64(final_client.ENCODEDKEY) }} as external_id,
        m_staff.id as created_by,
        m_staff.id as last_modified_by,
        final_client.BIRTHDATE as date_of_birth,
        final_client.CREATIONDATE as submittedon_date,
        {{ decode_base64(final_client.EMAILADDRESS) }} as email_address,
        {{ decode_base64(final_client.FIRSTNAME) }} as firstname,
        {{ decode_base64(final_client.MIDDLENAME) }} as middlename,
        {{ decode_base64(final_client.LASTNAME) }} as lastname,
        m_code_value.id as gender_cv_id,
        final_client.LASTMODIFIEDDATE as last_modified_on_utc,
        final_client.LASTMODIFIEDDATE as updated_on,
        {{ decode_base64(final_client.ID) }} as account_no,
        {{ decode_base64(final_client.MOBILEPHONE1) }} as mobile_no,
        m_office.id as office_id,
        CASE 
            WHEN final_client.STATE = 'ACTIVE' THEN 300
            WHEN final_client.STATE = 'EXITED' THEN 600
            WHEN final_client.STATE = 'REJECTED' THEN 700
            WHEN final_client.STATE = 'PENDING_APPROVAL' THEN 100
            WHEN final_client.STATE = 'BLACKLISTED' THEN 500
            ELSE 0
        END as status_enum,
        final_client.ACTIVATIONDATE as activation_date,
        final_client.CLOSEDDATE as closedon_date
    FROM {{ source('your_database', 'final_client') }} AS final_client
    LEFT JOIN {{ ref('m_staff') }} AS m_staff ON {{ decode_base64(final_client.ASSIGNEDUSERKEY) }} = m_staff.external_id
    LEFT JOIN {{ ref('m_code_value') }} AS m_code_value ON {{ decode_base64(final_client.GENDER) }} = m_code_value.code_value
    LEFT JOIN {{ ref('m_office') }} AS m_office ON {{ decode_base64(final_client.ASSIGNEDBRANCHKEY) }} = m_office.external_id
)

SELECT * FROM final_client_mapping;
