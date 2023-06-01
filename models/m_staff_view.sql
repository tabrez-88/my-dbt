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
),
user_with_decoded_keys AS (
    SELECT *,
        {{ decode_base64("assignedbranchkey") }} AS decoded_assignedbranchkey,
        {{ decode_base64("role_encodedkey_oid") }} AS decoded_role_encodedkey_oid
    FROM {{ ref('user') }}
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
    NULL AS organisational_role_parent_staff_id, 
    CASE WHEN u.USERSTATE = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active,
    u.CREATIONDATE AS joining_date,
    NULL AS image_id  */

FROM user_with_decoded_keys AS u
LEFT JOIN branch_office AS bo ON u.decoded_assignedbranchkey = bo.office_external_id
LEFT JOIN roles AS r ON u.decoded_role_encodedkey_oid = r.role_encoded_key
