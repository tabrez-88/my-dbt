{{ config(materialized='table') }}

WITH user_decoded AS (
    SELECT 
        u."ID" AS id,
        {{ decode_base64('u.ENCODEDKEY') }} AS user_external_id,
        {{ decode_base64('u.USERNAME') }} AS username,
        {{ decode_base64('u.FIRSTNAME') }} AS firstname,
        {{ decode_base64('u.LASTNAME') }} AS lastname,
        {{ decode_base64('u."PASSWORD"') }} AS password,
        {{ decode_base64('u.EMAIL') }} AS email
    FROM {{ ref('user') }} AS u
),
user_office AS (
    SELECT 
        ud.user_external_id,
        ud.id,
        ud.username,
        ud.firstname,
        ud.lastname,
        ud.password,
        ud.email,
        s.office_id
    FROM user_decoded AS ud
    LEFT JOIN {{ ref('m_staff') }} AS s ON ud.user_external_id = s.external_id
)

SELECT 
    uo.id::int8 as id,
    FALSE as is_deleted,
    uo.office_id,
    uo.id as staff_id,
    uo.username,
    uo.firstname,
    uo.lastname,
    uo.password,
    uo.email,
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