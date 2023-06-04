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
        {{ decode_base64("interestchargefrequency") }} as decoded_interestchargefrequency
    FROM {{ ref('final_loanaccount') }}
),

client_view AS (
    SELECT * 
    FROM m_client_view
),

group_view AS (
    SELECT * 
    FROM m_group_view
)

SELECT 
    b.external_id,
    b."ID" as account_no,
    CASE 
        WHEN b.decoded_accountholdertype = 'CLIENT' THEN cv.id
        WHEN b.decoded_accountholdertype = 'GROUP' THEN gv.id
        ELSE NULL
    END as client_id_group_id,
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
/*

{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    {{ decode_base64("encodedkey") }} as external_id,
    "ID" as account_no,
   (CASE 
        WHEN {{ decode_base64("accountholdertype") }} = 'CLIENT' THEN (SELECT id FROM m_client_view WHERE external_id = {{ decode_base64("accountholderkey") }})
        WHEN {{ decode_base64("accountholdertype") }} = 'GROUP' THEN (SELECT id FROM m_group_view WHERE external_id = {{ decode_base64("accountholderkey") }})
        ELSE NULL
    END) as client_id_group_id,
     {{ decode_base64("accountholdertype") }} as legal_form_id,
    {{ decode_base64("accountstate") }} as loan_status_id,
    {{ decode_base64("accountsubstate") }} as loan_sub_status_id,
    (SELECT id FROM m_office_view WHERE external_id = assignedbranchkey) as office_id,
    {{ decode_base64("assigneduserkey") }} as created_by,
    closeddate as closedon_date,
    creationdate as created_on_utc,
    approveddate as approvedon_date,
    graceperiod as grace_interest_free_periods,
    lastmodifieddate as last_modified_on_utc,
    loanamount as principal_amount,
    repaymentinstallments as number_of_repayments,
    repaymentperiodcount as repayment_period_frequency_enum,
    defaultfirstrepaymentduedateoffset as expected_firstrepaymenton_date,
    {{ decode_base64("interestcalculationmethod") }} as interest_method_enum,
    {{ decode_base64("interestchargefrequency") }} as interest_period_frequency_enum,
    interestbalance as interest_outstanding_derived,
    interestpaid as interest_repaid_derived,
    interestrate as nominal_interest_rate_per_period,
    interestdue as interest_charged_derived,
    feesdue as fee_charges_charged_derived,
    feespaid as fee_charges_repaid_derived
FROM {{ ref('final_loanaccount') }}*/
