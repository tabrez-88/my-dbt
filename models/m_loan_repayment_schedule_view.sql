-- models/m_loan_repayment_schedule.sql
SELECT
    ROW_NUMBER() OVER (ORDER BY ENCODEDKEY) as id,  -- Generate a new ID as it's not provided in source
    -- Need to handle loan_id as it's not provided in source
    CREATIONDATE as fromdate,
    DUEDATE as duedate,
    -- Need to handle installment as it's not provided in source
    PRINCIPALDUE as principal_amount,
    PRINCIPALPAID as principal_completed_derived,
    -- Need to handle principal_writtenoff_derived as it's not provided in source
    INTERESTDUE as interest_amount,
    INTERESTPAID as interest_completed_derived,
    -- Need to handle rest of the fields as they are not provided in source
FROM {{ source('public', 'repayment') }}
