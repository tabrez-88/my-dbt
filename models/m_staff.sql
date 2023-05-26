{{ config(materialized='table') }}

WITH branch_office AS (
    SELECT 
        decode_base64_or_text(external_id) AS office_external_id,
        id AS office_id
    FROM {{ ref('m_office') }}
),
roles AS (
    SELECT 
        "ID" AS role_id,
        decode_base64_or_text("NAME") AS organisational_role_enum,
        decode_base64_or_text(ENCODEDKEY) AS role_encoded_key
    FROM {{ ref('role') }}
),
user_with_decoded_branchkey AS (
    SELECT *,
        {{ decode_base64("assignedbranchkey") }} AS decoded_assignedbranchkey
    FROM {{ ref('user') }}
)

SELECT 
    u."ID" AS id,
    CASE WHEN u.ISCREDITOFFICER = '1' THEN TRUE ELSE FALSE END AS is_loan_officer,
    bo.office_id,
    decode_base64_or_text(u.FIRSTNAME) AS firstname,
    decode_base64_or_text(u.LASTNAME) AS lastname,
    decode_base64_or_text(u.USERNAME) AS display_name,
    decode_base64_or_text(u.MOBILEPHONE1) AS mobile_no,
    decode_base64_or_text(u.EMAIL) AS email_address,
    decode_base64_or_text(u.ENCODEDKEY) as external_id,
    r.organisational_role_enum,
    NULL AS organisational_role_parent_staff_id, -- Assuming there's no equivalent field in the source table
    CASE WHEN u.USERSTATE = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active,
    u.CREATIONDATE AS joining_date,
    NULL AS image_id  -- Assuming there's no equivalent field in the source table

FROM user_with_decoded_branchkey AS u
LEFT JOIN branch_office AS bo ON u.decoded_assignedbranchkey = bo.office_external_id
LEFT JOIN roles AS r ON decode_base64_or_text(u.ROLE_ENCODEDKEY_OID) = r.role_encoded_key
