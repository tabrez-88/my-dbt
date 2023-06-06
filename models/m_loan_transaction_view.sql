{% macro decode_base64(field) %}
    CONVERT_FROM(DECODE({{ field }}, 'base64'), 'utf-8')
{% endmacro %}

WITH type_mapping AS (
    SELECT * 
    FROM (VALUES 
        ('DISBURSEMENT', 'DISBURSMENT'),
        ('REPAYMENT', 'REPAYMENT'),
        ('WRITEOFF', 'WRITE_OFF'),
        ('INITIATE_TRANSFER', 'TRANSFER')
    ) AS v(original, mapped)
),
decoded_loantransaction AS (
    SELECT
        {{ decode_base64("encodedkey") }} AS external_id,
        AMOUNT AS amount,
        BALANCE AS outstanding_loan_balance_derived,
        creationdate AS created_date,
        {{ decode_base64("TYPE") }} AS transaction_type_raw,
        ENTRYDATE AS transaction_date,
        PRINCIPALAMOUNT AS principal_portion_derived,
        INTERESTAMOUNT AS interest_portion_derived,
        FEESAMOUNT AS fee_charges_portion_derived,
        PENALTYAMOUNT AS penalty_charges_portion_derived,
        transactionid as id,
        {{ decode_base64("branchkey") }} AS branch_key,
        {{ decode_base64("userkey") }} AS user_key
    FROM {{ ref('final_loantransaction') }}
),
loan_transactions AS (
    SELECT 
        dlt.external_id,
        dlt.id,
        dlt.amount,
        dlt.outstanding_loan_balance_derived,
        mv_office.id AS office_id,
        dlt.created_date,
        mv_loan.account_no AS loan_id,
        re.enum_id AS transaction_type_enum,
        mv_staff.id AS created_by,
        dlt.transaction_date,
        dlt.principal_portion_derived,
        dlt.interest_portion_derived,
        dlt.fee_charges_portion_derived,
        dlt.penalty_charges_portion_derived,
        tm.mapped AS transaction_type
    FROM decoded_loantransaction AS dlt
    LEFT JOIN type_mapping tm ON dlt.transaction_type_raw = tm.original
    LEFT JOIN {{ ref('m_office_view') }} AS mv_office ON dlt.branch_key = mv_office.external_id
    LEFT JOIN {{ ref('m_staff_view') }} AS mv_staff ON dlt.user_key = mv_staff.external_id
    LEFT JOIN {{ ref('m_loan_view') }} AS mv_loan ON dlt.external_id = mv_loan.external_id
    LEFT JOIN public.r_enum_value AS re ON (tm.mapped = re.enum_message_property AND re.enum_name = 'loan_transaction_type_enum')
)

SELECT 
    lt.*
FROM loan_transactions AS lt
LEFT JOIN {{ ref('m_loan_view') }} AS mv_loan ON lt.external_id = mv_loan.external_id
