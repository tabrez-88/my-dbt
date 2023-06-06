WITH decoded_repayment AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ENCODEDKEY) as id,
        {{ decode_base64("parentaccountkey") }} AS parentaccountkey,
        CREATIONDATE as fromdate,
        DUEDATE as duedate,
        PRINCIPALDUE as principal_amount,
        PRINCIPALPAID as principal_completed_derived,
        INTERESTDUE as interest_amount,
        INTERESTPAID as interest_completed_derived
    FROM 'repayment'
),
repayment_with_loan_id AS (
    SELECT 
        dr.id,
        mv_loan.account_no AS loan_id,
        dr.fromdate,
        dr.duedate,
        dr.principal_amount,
        dr.principal_completed_derived,
        dr.interest_amount,
        dr.interest_completed_derived
    FROM decoded_repayment AS dr
    LEFT JOIN {{ ref('m_loan_view') }} AS mv_loan ON dr.parentaccountkey = mv_loan.external_id
)

SELECT * FROM repayment_with_loan_id
