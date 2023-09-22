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
mv_office AS (
   

    SELECT id,external_id
    FROM m_office
),
mv_staff AS (

    SELECT id,external_id
    FROM m_staff
),
mv_loan AS (

    SELECT id,external_id
    FROM m_loan
),
decoded_loantransaction AS (
    SELECT
        "encodedkey" AS external_id,
        AMOUNT AS amount,
        BALANCE AS outstanding_loan_balance_derived,
        "parentaccountkey" AS parent_id,
        creationdate AS created_date,
        "TYPE" AS transaction_type_raw,
        ENTRYDATE AS transaction_date,
        PRINCIPALAMOUNT AS principal_portion_derived,
        INTERESTAMOUNT AS interest_portion_derived,
        FEESAMOUNT AS fee_charges_portion_derived,
        PENALTYAMOUNT AS penalty_charges_portion_derived,
        transactionid as id,
        "branchkey" AS branch_key,
        "userkey" AS user_key
    FROM final_loantransaction
),
loan_transactions AS (
    SELECT 
        dlt.external_id,
        dlt.id,
        COALESCE(dlt.amount, 0) as amount,
        dlt.outstanding_loan_balance_derived,
        mv_office.id AS office_id,
        dlt.created_date,
        mv_loan.id AS loan_id,
        CASE 
            WHEN dlt.transaction_type_raw = 'BRANCH_CHANGED' THEN 3
            WHEN dlt.transaction_type_raw = 'DEFERRED_INTEREST_APPLIED' THEN 19
            WHEN dlt.transaction_type_raw = 'DEFERRED_INTEREST_APPLIED_ADJUSTMENT' THEN 19
            WHEN dlt.transaction_type_raw = 'DEFERRED_INTEREST_PAID' THEN 2
            WHEN dlt.transaction_type_raw = 'DEFERRED_INTEREST_PAID_ADJUSTMENT' THEN 10
            WHEN dlt.transaction_type_raw = 'DISBURSMENT' THEN 1
            WHEN dlt.transaction_type_raw = 'DISBURSMENT_ADJUSTMENT' THEN 5
            WHEN dlt.transaction_type_raw = 'FEE' THEN 10
            WHEN dlt.transaction_type_raw = 'FEE_ADJUSTMENT' THEN 10
            WHEN dlt.transaction_type_raw = 'FEE_CHARGED' THEN 10
            WHEN dlt.transaction_type_raw = 'IMPORT' THEN 1
            WHEN dlt.transaction_type_raw = 'INTEREST_APPLIED' THEN 19
            WHEN dlt.transaction_type_raw = 'INTEREST_APPLIED_ADJUSTMENT' THEN 19
            WHEN dlt.transaction_type_raw = 'INTEREST_DUE_REDUCED' THEN 4
            WHEN dlt.transaction_type_raw = 'INTEREST_LOCKED' THEN 19
            WHEN dlt.transaction_type_raw = 'INTEREST_UNLOCKED' THEN 19
            WHEN dlt.transaction_type_raw = 'PENALTIES_DUE_REDUCED' THEN 9
            WHEN dlt.transaction_type_raw = 'PENALTY_ADJUSTMENT' THEN 9
            WHEN dlt.transaction_type_raw = 'PENALTY_APPLIED' THEN 10
            WHEN dlt.transaction_type_raw = 'REPAYMENT' THEN 2
            WHEN dlt.transaction_type_raw = 'REPAYMENT_ADJUSTMENT' THEN 2
            WHEN dlt.transaction_type_raw = 'TRANSFER' THEN 7
            WHEN dlt.transaction_type_raw = 'TRANSFER_ADJUSTMENT' THEN 2
            WHEN dlt.transaction_type_raw = 'WRITE_OFF' THEN 6
            WHEN dlt.transaction_type_raw = 'WRITE_OFF_ADJUSTMENT' THEN 6
        END AS transaction_type_enum,
        mv_staff.id AS created_by,
        dlt.transaction_date,
        dlt.principal_portion_derived,
        dlt.interest_portion_derived,
        dlt.fee_charges_portion_derived,
        dlt.penalty_charges_portion_derived,
        mv_loan.external_id as loan_external_id
    FROM decoded_loantransaction AS dlt
    LEFT JOIN type_mapping tm ON dlt.transaction_type_raw = tm.original
    LEFT JOIN  mv_office ON dlt.branch_key = mv_office.external_id
    LEFT JOIN  mv_staff ON dlt.user_key = mv_staff.external_id
    LEFT JOIN  mv_loan ON dlt.parent_id = mv_loan.external_id
)

SELECT 
    lt.*
FROM loan_transactions AS lt
LEFT JOIN  mv_loan ON lt.external_id = mv_loan.external_id
