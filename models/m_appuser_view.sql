{{ config(materialized='table') }}

WITH decoded_user AS (
    SELECT 
        {{ decode_base64("encodedkey") }} AS user_external_id,
        "ID" AS id,
        {{ decode_base64("username") }} AS username,
        {{ decode_base64("firstname") }} AS firstname,
        {{ decode_base64("lastname") }} AS lastname,
        {{ decode_base64("PASSWORD") }} AS password,
        {{ decode_base64("email") }} AS email
    FROM {{ ref('user') }}
),
user_office AS (
    SELECT 
        du.user_external_id,
        du.id,
        du.username,
        du.firstname,
        du.lastname,
        du.password,
        du.email,
        s.office_id
    FROM decoded_user AS du
    LEFT JOIN {{ ref('m_staff_view') }} AS s ON du.user_external_id = s.external_id
)

SELECT 
    uo.id::int8 as id,
    FALSE as is_deleted,
    COALESCE(uo.office_id, 1) as office_id,
    uo.id as staff_id,
    uo.username,
    COALESCE(uo.firstname, '') as firstname,
    COALESCE(uo.lastname, '') as lastname,
    COALESCE(uo.password, '') as password,
    COALESCE(uo.email, '') as email,
    TRUE as firsttime_login_remaining,
    TRUE as nonexpired,
    TRUE as nonlocked,
    TRUE as nonexpired_credentials,
    TRUE as enabled,
    CURRENT_DATE as last_time_password_updated,
    FALSE as password_never_expires,
    FALSE as is_self_service_user,
    FALSE as cannot_change_password
FROM user_office AS uo
