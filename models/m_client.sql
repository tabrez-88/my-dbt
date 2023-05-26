{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        convert_from(decode(fcl."encodedkey", 'base64'), 'UTF-8') as external_id,
        convert_from(decode(fcl."assigneduserkey", 'base64'), 'UTF-8') as created_by,
        convert_from(decode(fcl."assigneduserkey", 'base64'), 'UTF-8')as last_modified_by,
        fcl."birthdate" as date_of_birth,
        fcl."creationdate" as submittedon_date,
        fcl."emailaddress" as email_address,
        convert_from(decode(fcl."firstname", 'base64'), 'UTF-8') as firstname,
        fcl."middlename" as middlename,
        convert_from(decode(fcl."lastname", 'base64'), 'UTF-8') as lastname,
        (SELECT id FROM m_code_value WHERE code_value = convert_from(decode(fcl."gender", 'base64'), 'UTF-8') )  as gender_cv_id,
        fcl."lastmodifieddate" as last_modified_on_utc,
        fcl."lastmodifieddate" as updated_on,
        convert_from(decode(fcl."profilepicturekey", 'base64'), 'UTF-8') as image_id,
        convert_from(decode(fcl."profilesignaturekey", 'base64'), 'UTF-8') as signature_id,
        convert_from(decode(fcl."ID", 'base64'), 'UTF-8') as account_no,
        convert_from(decode(fcl."mobilephone1", 'base64'), 'UTF-8') as mobile_no,
        convert_from(decode(fcl."assignedbranchkey", 'base64'), 'UTF-8') as office_id,
        CASE 
            WHEN convert_from(decode(fcl."STATE", 'base64'), 'UTF-8') = 'ACTIVE' THEN 300 
            WHEN convert_from(decode(fcl."STATE", 'base64'), 'UTF-8') = 'EXITED' THEN 600
            WHEN convert_from(decode(fcl."STATE", 'base64'), 'UTF-8') = 'REJECTED' THEN 700
            WHEN convert_from(decode(fcl."STATE", 'base64'), 'UTF-8') = 'PENDING_APPROVAL' THEN 100
            WHEN convert_from(decode(fcl."STATE", 'base64'), 'UTF-8') = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        fcl."activationdate" as activation_date,
        fcl."closeddate" as closedon_date
        FROM {{ ref('final_client') }} as fcl
)
SELECT * FROM base
