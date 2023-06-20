{{
    config(
        materialized = 'table'
    )
}}

WITH base AS (
    SELECT *,
        {{ decode_base64("encodedkey") }} as external_id,
        {{ decode_base64("accountholdertype") }} as decoded_accountholdertype,
        {{ decode_base64("accountholderkey") }} as decoded_accountholderkey,
        {{ decode_base64("interestcalculationmethod") }} as decoded_interestcalculationmethod,
        {{ decode_base64("interestchargefrequency") }} as decoded_interestchargefrequency,
        {{ decode_base64("assignedbranchkey") }} as decoded_assignedbranchkey,
        {{ decode_base64("assigneduserkey") }} as decoded_assigneduserkey,
        {{ decode_base64("repaymentperiodunit") }} as decoded_repaymentperiodunit,
        {{ decode_base64("accountstate") }} as decoded_accountstate
    FROM {{ ref('final_loanaccount') }}
),

client_view AS (
    SELECT * 
    FROM m_client_view
),

group_view AS (
    SELECT * 
    FROM m_group_view
),
office_view AS (
    SELECT * 
    FROM m_office_view
),
staff_view AS (
    SELECT *
    FROM m_staff_view
)

SELECT 
    b.external_id,
    b."ID" as account_no,
    CASE WHEN b.decoded_accountholdertype = 'CLIENT' THEN cv.id ELSE NULL END as client_id, 
    CASE WHEN b.decoded_accountholdertype = 'GROUP' THEN gv.id ELSE NULL END as group_id, 
    CASE 
        WHEN b.decoded_accountholdertype = 'CLIENT' THEN 1 
        WHEN b.decoded_accountholdertype = 'GROUP' THEN 2 
        ELSE NULL 
    END as loan_type_enum,
    CASE
        WHEN b.decoded_interestcalculationmethod  = 'FLAT' THEN 0
        WHEN b.decoded_interestcalculationmethod IN ('DECLINING_BALANCE_DISCOUNTED', 'DECLINING_BALANCE') THEN 1
        ELSE NULL
    END as  interest_method_enum,  
    
    ov.id as office_id,
    sv.id as created_by,
    b.closeddate as closedon_date,
    b.creationdate as created_on_utc,
    b.approveddate as approvedon_date,
    b.graceperiod as grace_interest_free_periods,
    b.lastmodifieddate as last_modified_on_utc,
    b.loanamount as principal_amount,
    b.repaymentinstallments as number_of_repayments,
    b.defaultfirstrepaymentduedateoffset as expected_firstrepaymenton_date,
    CASE 
        WHEN b.decoded_repaymentperiodunit = 'DAYS' THEN 0 
        WHEN b.decoded_repaymentperiodunit = 'WEEKS' THEN 1 
        WHEN b.decoded_repaymentperiodunit IN ('YEARS', 'MONTHS') THEN 2
    ELSE NULL 
    END as repayment_period_frequency_enum,
    CASE
        WHEN b.decoded_repaymentperiodunit = 'YEARS' THEN b.repaymentperiodcount*12
        ELSE b.repaymentperiodcount
    END as repay_every,
    CASE 
        WHEN b.decoded_repaymentperiodunit = 'DAYS' THEN 0 
        WHEN b.decoded_repaymentperiodunit = 'WEEKS' THEN 1 
        WHEN b.decoded_repaymentperiodunit = 'MONTHS' THEN 2
        WHEN b.decoded_repaymentperiodunit = 'YEARS' THEN 3
    ELSE NULL 
    END as term_period_frequency_enum,

    CASE
        WHEN b.decoded_accountstate = 'ACTIVE' THEN 300
        WHEN b.decoded_accountstate = 'ACTIVE_IN_ARREARS' THEN 300
        WHEN b.decoded_accountstate = 'APPROVED' THEN 200
        WHEN b.decoded_accountstate = 'CLOSED' THEN 600
        WHEN b.decoded_accountstate = 'CLOSED_REJECTED' THEN 500
        WHEN b.decoded_accountstate = 'CLOSED_WRITTEN_OFF' THEN 601
        WHEN b.decoded_accountstate = 'PARTIAL_APPLICATION' THEN 0
        WHEN b.decoded_accountstate = 'PENDING_APPROVAL' THEN 100
    ELSE NULL
    END as loan_status_id,
    CASE
        WHEN b.accountsubstate = 'WITHDRAWN' THEN 400
        WHEN b.accountsubstate = 'REFINANCED' THEN 600
        WHEN b.accountsubstate = 'RESCHEDULED' THEN 602
    ELSE NULL
    END as loan_sub_status_id,

     CASE
        WHEN b.decoded_interestchargefrequency = 'ANNUALIZED' THEN 3
        WHEN b.decoded_interestchargefrequency IN ('EVERY_MONTH','EVERY_FOUR_WEEKS') THEN 2
        WHEN b.decoded_interestchargefrequency = 'EVERY_DAY' THEN 1
    ELSE NULL
    END as interest_period_frequency_enum,

    b.interestbalance as interest_outstanding_derived,
    b.interestpaid as interest_repaid_derived,
    b.interestrate as nominal_interest_rate_per_period,
    b.interestdue as interest_charged_derived,
    b.feesdue as fee_charges_charged_derived,
    b.feespaid as fee_charges_repaid_derived
FROM base b
LEFT JOIN client_view cv
    ON b.decoded_accountholderkey = cv.external_id
LEFT JOIN group_view gv
    ON b.decoded_accountholderkey = gv.external_id
LEFT JOIN office_view ov 
    ON b.decoded_assignedbranchkey = ov.external_id    
LEFT JOIN staff_view sv 
    ON b.decoded_assigneduserkey = sv.external_id    
