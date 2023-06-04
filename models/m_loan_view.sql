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
        {{ decode_base64("assigneduserkey") }} as decoded_assigneduserkey
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
    CASE 
        WHEN b.decoded_accountholdertype = 'CLIENT' THEN cv.id
        WHEN b.decoded_accountholdertype = 'GROUP' THEN gv.id
        ELSE NULL
    END as client_id_group_id,
    ov.id as office_id,
    sv.id as created_by,
    b.closeddate as closedon_date,
    b.creationdate as created_on_utc,
    b.approveddate as approvedon_date,
    b.graceperiod as grace_interest_free_periods,
    b.lastmodifieddate as last_modified_on_utc,
    b.loanamount as principal_amount,
    b.repaymentinstallments as number_of_repayments,
    b.repaymentperiodcount as repayment_period_frequency_enum,
    b.defaultfirstrepaymentduedateoffset as expected_firstrepaymenton_date,
    b.decoded_interestcalculationmethod as interest_method_enum,
    b.decoded_interestchargefrequency as interest_period_frequency_enum,
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
/*
     {{ decode_base64("accountholdertype") }} as legal_form_id,
    {{ decode_base64("accountstate") }} as loan_status_id,
    {{ decode_base64("accountsubstate") }} as loan_sub_status_id*/
