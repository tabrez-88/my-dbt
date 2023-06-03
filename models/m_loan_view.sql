{% set source_table = 'public.final_loanaccount' %}

{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    {{ decode_base64("encodedkey") }} as external_id,
    {{ decode_base64("ID") }} as account_no,
    (CASE 
        WHEN {{ decode_base64("accountholdertype") }} = "CLIENT" THEN (SELECT id FROM m_client_view WHERE external_id = {{ decode_base64("accountholderkey") }})
        WHEN {{ decode_base64("accountholdertype") }} = "GROUP" THEN (SELECT id FROM m_group_view WHERE external_id = {{ decode_base64("accountholderkey") }})
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
FROM {{ source_table }}
