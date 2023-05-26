{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        decode_base64(fcl."encodedkey") as external_id,
        decode_base64(fcl."assigneduserkey") as created_by,
        decode_base64(fcl."assigneduserkey") as last_modified_by,
        fcl."birthdate" as date_of_birth,
        fcl."creationdate" as submittedon_date,
        decode_base64(fcl."emailaddress") as email_address,
        decode_base64(fcl."firstname") as firstname,
        decode_base64(fcl."middlename") as middlename,
        decode_base64(fcl."lastname") as lastname,
        (SELECT id FROM m_code_value WHERE code_value = decode_base64(fcl."gender")) as gender_cv_id,
        fcl."lastmodifieddate" as last_modified_on_utc,
        fcl."lastmodifieddate" as updated_on,
        decode_base64(fcl."profilepicturekey") as image_id,
        decode_base64(fcl."profilesignaturekey") as image_id,
        decode_base64(fcl."ID") as account_no,
        decode_base64(fcl."mobilephone1") as mobile_no,
        decode_base64(fcl."assignedbranchkey") as office_id,
        CASE 
            WHEN decode_base64(fcl."STATE") = 'ACTIVE' THEN 300 
            WHEN decode_base64(fcl."STATE") = 'EXITED' THEN 600
            WHEN decode_base64(fcl."STATE") = 'REJECTED' THEN 700
            WHEN decode_base64(fcl."STATE") = 'PENDING_APPROVAL' THEN 100
            WHEN decode_base64(fcl."STATE") = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        fcl."activationdate" as activation_date,
        fcl."closeddate" as closedon_date
        FROM {{ ref('final_client') }} as fcl
)
SELECT * FROM base
