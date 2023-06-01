{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        fcl."encodedkey" as external_id,
        fcl."assigneduserkey" as created_by,
        fcl."assigneduserkey"as last_modified_by,
        fcl."birthdate" as date_of_birth,
        fcl."creationdate" as submittedon_date,
        fcl."emailaddress" as email_address,
        fcl."firstname" as firstname,
        fcl."middlename" as middlename,
        fcl."lastname" as lastname,
        (SELECT id FROM m_code_value WHERE code_value = fcl."gender")  as gender_cv_id,
        fcl."lastmodifieddate" as last_modified_on_utc,
        fcl."lastmodifieddate" as updated_on,
        fcl."profilepicturekey" as image_id,
        fcl."profilesignaturekey" as signature_id,
        fcl."ID" as account_no,
        fcl."mobilephone1" as mobile_no,
        fcl."assignedbranchkey" as office_id,
        CASE 
            WHEN fcl."STATE" = 'ACTIVE' THEN 300 
            WHEN fcl."STATE" = 'EXITED' THEN 600
            WHEN fcl."STATE" = 'REJECTED' THEN 700
            WHEN fcl."STATE" = 'PENDING_APPROVAL' THEN 100
            WHEN fcl."STATE" = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        fcl."activationdate" as activation_date,
        fcl."closeddate" as closedon_date
        FROM {{ ref('final_client') }} as fcl
)
SELECT * FROM base
