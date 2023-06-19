{{ config(materialized='table') }}

WITH branch_office AS (
    SELECT 
        external_id AS office_external_id,
        id AS office_id
    FROM {{ ref('m_office_view') }}
),
roles AS (
    SELECT 
        CAST(rv.id AS int2) AS organisational_role_enum,
        {{ decode_base64("encodedkey") }} AS role_encoded_key
    FROM {{ ref('role') }}
    LEFT JOIN {{ ref('m_role_view') }} AS rv ON rv.name = {{ decode_base64("NAME") }}
),
user_with_decoded_keys AS (
    SELECT *,
        {{ decode_base64("assignedbranchkey") }} AS decoded_assignedbranchkey,
        {{ decode_base64("role_encodedkey_oid") }} AS decoded_role_encodedkey_oid,
        {{ decode_base64("firstname") }} AS decoded_firstname,
        {{ decode_base64("lastname") }} AS decoded_lastname,
        {{ decode_base64("username") }} AS decoded_username,
        {{ decode_base64("mobilephone1") }} AS decoded_mobile_no,
        {{ decode_base64("email") }} AS decoded_email,
        {{ decode_base64("encodedkey") }} AS decoded_external_id,
        CASE WHEN {{ decode_base64("userstate") }} = 'ACTIVE' THEN TRUE ELSE FALSE END AS decoded_is_active
    FROM {{ ref('user') }}
)

SELECT 
    u."ID" AS id,
    CASE WHEN u.ISCREDITOFFICER = '1' THEN TRUE ELSE FALSE END AS is_loan_officer,
    r.organisational_role_enum,
    bo.office_id,
    u.decoded_firstname AS firstname,
    u.decoded_lastname AS lastname,
    u.decoded_username AS display_name,
    u.decoded_mobile_no AS mobile_no,
    u.decoded_email AS email_address,
    u.decoded_external_id as external_id,
    CAST(NULL AS int8) AS organisational_role_parent_staff_id,
    u.decoded_is_active AS is_active,
    CAST(u.CREATIONDATE AS date) AS joining_date,
    CAST(NULL AS int8) AS image_id  -- Assuming there's no equivalent field in the source table

FROM user_with_decoded_keys AS u
LEFT JOIN branch_office AS bo ON u.decoded_assignedbranchkey = bo.office_external_id
LEFT JOIN roles AS r ON u.decoded_role_encodedkey_oid = r.role_encoded_key
