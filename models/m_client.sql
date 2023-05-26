{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        decode_base64(fcl."ENCODEDKEY") as external_id,
        decode_base64(fcl."ASSIGNEDUSERKEY") as created_by,
        decode_base64(fcl."ASSIGNEDUSERKEY") as last_modified_by,
        fcl."BIRTHDATE" as date_of_birth,
        fcl."CREATIONDATE" as submittedon_date,
        decode_base64(fcl."EMAILADDRESS") as email_address,
        decode_base64(fcl."FIRSTNAME") as firstname,
        decode_base64(fcl."MIDDLENAME") as middlename,
        decode_base64(fcl."LASTNAME") as lastname,
        (SELECT id FROM m_code_value WHERE code_value = decode_base64(fcl."GENDER")) as gender_cv_id,
        fcl."LASTMODIFIEDDATE" as last_modified_on_utc,
        fcl."LASTMODIFIEDDATE" as updated_on,
        decode_base64(fcl."PROFILEPICTUREKEY") as image_id,
        decode_base64(fcl."PROFILESIGNATUREKEY") as image_id,
        decode_base64(fcl."ID") as account_no,
        decode_base64(fcl."MOBILEPHONE1") as mobile_no,
        decode_base64(fcl."ASSIGNEDBRANCHKEY") as office_id,
        CASE 
            WHEN decode_base64(fcl."STATE") = 'ACTIVE' THEN 300 
            WHEN decode_base64(fcl."STATE") = 'EXITED' THEN 600
            WHEN decode_base64(fcl."STATE") = 'REJECTED' THEN 700
            WHEN decode_base64(fcl."STATE") = 'PENDING_APPROVAL' THEN 100
            WHEN decode_base64(fcl."STATE") = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        fcl."ACTIVATIONDATE" as activation_date,
        fcl."CLOSEDDATE" as closedon_date
        FROM {{ ref('final_client') }} as fcl
)
SELECT * FROM base
