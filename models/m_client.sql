{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        decode_base64_or_text(fcl."encodedkey") as external_id,
        decode_base64_or_text(fcl."assigneduserkey") as created_by,
        decode_base64_or_text(fcl."assigneduserkey")as last_modified_by,
        fcl."birthdate" as date_of_birth,
        fcl."creationdate" as submittedon_date,
        fcl."emailaddress" as email_address,
        decode_base64_or_text(fcl."firstname") as firstname,
        fcl."middlename" as middlename,
        decode_base64_or_text(fcl."lastname") as lastname,
        (SELECT id FROM m_code_value WHERE code_value = decode_base64_or_text(fcl."gender"))  as gender_cv_id,
        fcl."lastmodifieddate" as last_modified_on_utc,
        fcl."lastmodifieddate" as updated_on,
        decode_base64_or_text(fcl."profilepicturekey") as image_id,
        decode_base64_or_text(fcl."profilesignaturekey") as signature_id,
        decode_base64_or_text(fcl."ID") as account_no,
        decode_base64_or_text(fcl."mobilephone1") as mobile_no,
        decode_base64_or_text(fcl."assignedbranchkey") as office_id,
        CASE 
            WHEN decode_base64_or_text(fcl."STATE") = 'ACTIVE' THEN 300 
            WHEN decode_base64_or_text(fcl."STATE") = 'EXITED' THEN 600
            WHEN decode_base64_or_text(fcl."STATE") = 'REJECTED' THEN 700
            WHEN decode_base64_or_text(fcl."STATE") = 'PENDING_APPROVAL' THEN 100
            WHEN decode_base64_or_text(fcl."STATE") = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        fcl."activationdate" as activation_date,
        fcl."closeddate" as closedon_date
        FROM {{ ref('final_client') }} as fcl
)
SELECT * FROM base
