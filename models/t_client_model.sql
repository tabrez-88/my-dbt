-- your_dbt_model.sql
WITH transformed AS (
  SELECT
    encodedkey::bigint AS id,
    assigneduserkey::bigint AS created_by,
    assigneduserkey::bigint AS last_modified_by,
    birthdate::date AS date_of_birth,
    creationdate::date AS submittedon_date,
    emailaddress AS email_address,
    firstname AS firstname,
    middlename AS middlename,
    lastname AS lastname,
    CASE
        WHEN gender = 'male' THEN 1 -- Replace with the appropriate code value ID
        WHEN gender = 'female' THEN 2 -- Replace with the appropriate code value ID
        ELSE NULL
    END AS gender_cv_id,
    lastmodifieddate::date AS last_modified_on_utc,
    lastmodifieddate::date AS updated_on,
    profilepicturekey::bigint AS image_id,
    profilesignaturekey::bigint AS image_id,
    id AS account_no,
    mobilephone1 AS mobile_no,
    assignedbranchkey::bigint AS office_id,
    CASE
        WHEN "STATE" = 'ACTIVE' THEN 300 -- Replace with the appropriate status_enum value
        WHEN "STATE" = 'EXITED' THEN 301 -- Replace with the appropriate status_enum value
        WHEN "STATE" = 'REJECTED' THEN 302 -- Replace with the appropriate status_enum value
        WHEN "STATE" = 'PENDING_APPROVAL' THEN 303 -- Replace with the appropriate status_enum value
        WHEN "STATE" = 'BLACKLISTED' THEN 304 -- Replace with the appropriate status_enum value
        ELSE NULL
    END AS status_enum,
    activationdate::date AS activation_date,
    closeddate::date AS closedon_date
  FROM 
     "public"."final_client" -- This assumes you have a dbt source or model named final_client
)

SELECT * FROM transformed
