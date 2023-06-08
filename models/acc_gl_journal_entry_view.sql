WITH decoded_gljournalentry AS (
    SELECT
        {{ decode_base64("encodedkey") }} AS encodedkey,
        {{ decode_base64("glaccount_encodedkey_oid") }} AS glaccount_encodedkey_oid,
        entrydate AS entrydate,
        {{ decode_base64("transactionid") }} AS transactionid,
        {{ decode_base64("producttype") }} AS producttype,
        {{ decode_base64("notes") }} AS notes,
        {{ decode_base64("userkey") }} AS userkey,
        amount,
        {{ decode_base64("reversalentrykey") }} AS reversalentrykey,
        {{ decode_base64("accountkey") }} AS accountkey,
        {{ decode_base64("TYPE") }} AS TYPE,
        {{ decode_base64("assignedbranchkey") }} AS assignedbranchkey,
        created_on_utc
    FROM {{ ref('gljournalentry') }}
),
transformed_gljournalentry AS (
    SELECT
        entryid AS id,
        created_on_utc,
        COALESCE(rv.enum_id, 0) AS type_enum,
        amount,
        notes AS description,
        transactionid,
        COALESCE(staff_view.id, 0) AS created_by,
        COALESCE(office_view.id, 0) AS office_id,
        COALESCE(reversal.gl_journal_entry_id, 0) AS reversal_id,
        CASE
            WHEN reversal.gl_journal_entry_id IS NULL THEN false
            ELSE true
        END AS reversed,
        COALESCE(loan_view.id, savings_view.id, 0) AS account_id
    FROM decoded_gljournalentry AS gle
    LEFT JOIN r_enum_value AS rv ON rv.enum_name = 'journal_entry_type_type_enum' AND rv.enum_message_property = gle.TYPE
    LEFT JOIN r_enum_value AS rv_account_type_enum ON rv_account_type_enum.enum_name = 'entity_account_type_enum' AND rv_account_type_enum.enum_message_property = gle.productkey
    LEFT JOIN m_loan_view AS loan_view ON loan_view.external_id = gle.accountkey AND gle.producttype = 'LOAN'
    LEFT JOIN m_saving_account AS savings_view ON savings_view.external_id = gle.accountkey AND gle.producttype = 'SAVINGS'
    LEFT JOIN m_staff_view AS staff_view ON staff_view.external_id = gle.userkey
    LEFT JOIN m_office_view AS office_view ON office_view.external_id = gle.assignedbranchkey
    LEFT JOIN (
        SELECT encodedkey AS reversalentrykey, id AS gl_journal_entry_id
        FROM gljournalentry
    ) AS reversal ON reversal.reversalentrykey = gle.reversalentrykey
)

SELECT
    id,
    account_id,
    office_id,
    reversal_id,
    currency_code,
    transaction_id,
    loan_transaction_id,
    savings_transaction_id,
    client_transaction_id,
    reversed,
    ref_num,
    manual_entry,
    entry_date,
    type_enum,
    amount,
    description,
    entity_type_enum,
    entity_id,
    created_by,
    last_modified_by,
    created_date,
    lastmodified_date,
    is_running_balance_calculated,
    office_running_balance,
    organization_running_balance,
    payment_details_id,
    share_transaction_id,
    transaction_date,
    created_on_utc,
    last_modified_on_utc,
    submitted_on_date
FROM transformed_gljournalentry
