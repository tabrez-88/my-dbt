{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT 
        ROW_NUMBER() OVER () as id,
        {{ decode_base64("encodedkey") }} as external_id,
        {{ decode_base64("assigneduserkey") }} as assigneduserkey,
        "birthdate" as date_of_birth,
        "creationdate" as submittedon_date,
        {{ decode_base64("firstname") }} as firstname,
        {{ decode_base64("lastname") }} as lastname,
        (SELECT id FROM m_code_value WHERE code_value = {{ decode_base64("gender") }})  as gender_cv_id,
        "lastmodifieddate" as last_modified_on_utc,
        "lastmodifieddate" as updated_on,
        "ID" as account_no,
        {{ decode_base64("mobilephone1") }} as mobile_no,
        COALESCE(o.id, 1) as office_id,
        CASE 
            WHEN {{ decode_base64("STATE") }} = 'ACTIVE' THEN 300 
            WHEN {{ decode_base64("STATE") }} = 'EXITED' THEN 600
            WHEN {{ decode_base64("STATE") }} = 'REJECTED' THEN 700
            WHEN {{ decode_base64("STATE") }} = 'PENDING_APPROVAL' THEN 100
            WHEN {{ decode_base64("STATE") }} = 'BLACKLISTED' THEN 400
            ELSE 0 
        END as status_enum,
        "activationdate" as activation_date,
        "closeddate" as closedon_date
    FROM {{ ref('final_client') }} c
    LEFT JOIN m_office_view o ON o.external_id = c.assignedbranchkey
), 
m_client as (
    SELECT  b.*, s1.id as created_by,s1.id as last_modified_by 
    from base b 
    join m_staff_view s1 on b.assigneduserkey = s1.external_id -- corrected join condition
)
SELECT * FROM m_client
