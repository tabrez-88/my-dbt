{{ config(materialized='table') }}

WITH branch_office AS (
    SELECT 
        external_id AS office_external_id,
        id AS office_id
    FROM {{ ref('m_office_view') }}
),
roles AS (
    SELECT 
        {{ decode_base64("ID") }} AS role_id,
        {{ decode_base64("NAME") }} AS organisational_role_enum,
        {{ decode_base64("encodedkey") }} AS role_encoded_key
    FROM {{ ref('role') }}
)

SELECT 
    u."ID" AS id,
    CASE WHEN u.ISCREDITOFFICER = '1' THEN TRUE ELSE FALSE END AS is_loan_officer,
    r.organisational_role_enum
   /* bo.office_id,
    u.FIRSTNAME AS firstname,
    u.LASTNAME AS lastname,
    u.USERNAME AS display_name,
    u.MOBILEPHONE1 AS mobile_no,
    u.EMAIL AS email_address,
    u.ENCODEDKEY as external_id,

    NULL AS organisational_role_parent_staff_id, -- Assuming there's no equivalent field in the source table
    CASE WHEN u.USERSTATE = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active,
    u.CREATIONDATE AS joining_date,
    NULL AS image_id  -- Assuming there's no equivalent field in the source table*/

FROM {{ ref('user') }} AS u
LEFT JOIN branch_office AS bo ON decode_base64(u.assignedbranchkey) = bo.office_external_id
LEFT JOIN roles AS r ON decode_base64(u.role_encodedkey_oid) = r.role_encoded_key
