{{ config(materialized='table') }}

WITH branch_office AS (
    SELECT 
        "ENCODEDKEY" AS office_external_id,
        id AS office_id
    FROM {{ ref('m_office') }}
)

SELECT 
    u.ID AS id,
    CASE WHEN u.ISCREDITOFFICER = b'1' THEN TRUE ELSE FALSE END AS is_loan_officer,
    bo.office_id,
    u.FIRSTNAME AS firstname,
    u.LASTNAME AS lastname,
    u.USERNAME AS display_name,
    u.MOBILEPHONE1 AS mobile_no,
    u.EMAIL AS email_address,
    NULL AS organisational_role_enum, -- Since we're ignoring ROLE_ENCODEDKEY_OID
    NULL AS organisational_role_parent_staff_id, -- Assuming there's no equivalent field in the source table
    CASE WHEN u.USERSTATE = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active,
    u.CREATIONDATE AS joining_date,
    NULL AS image_id  -- Assuming there's no equivalent field in the source table

FROM {{ ref('user') }} AS u
LEFT JOIN branch_office AS bo ON u.ASSIGNEDBRANCHKEY = bo.office_external_id
