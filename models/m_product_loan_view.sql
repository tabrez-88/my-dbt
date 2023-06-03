WITH source AS (
    SELECT
        "ID" as id,
        lp.ENCODEDKEY as external_id,
        PRODUCTNAME AS name,
        CURRENCYCODE AS currency_code,
        DEFAULTLOANAMOUNT as principal_amount,
        MINLOANAMOUNT as min_principal_amount,
        MAXLOANAMOUNT as max_principal_amount,
        PRODUCTDESCRIPTION AS description,
        ips.DEFAULTINTERESTRATE AS nominal_interest_rate_per_period,
        ips.MININTERESTRATE AS min_nominal_interest_rate_per_period,
        ips.MAXINTERESTRATE AS max_nominal_interest_rate_per_period,
        CASE
            WHEN INTERESTCALCULATIONMETHOD = 'FLAT' THEN 1
            ELSE 0 
        END as interest_method_enum,
        CASE
            WHEN DEFAULTREPAYMENTPERIODCOUNT IS NULL THEN 1
            ELSE DEFAULTREPAYMENTPERIODCOUNT
        END as repay_every,
        CASE
            WHEN REPAYMENTPERIODUNIT = 'DAYS' THEN 0
            WHEN REPAYMENTPERIODUNIT = 'WEEKS' THEN 1
            ELSE 2 -- Default to months
        END as repayment_period_frequency_enum,
        CASE
            WHEN DEFAULTNUMINSTALLMENTS IS NULL THEN 3
            ELSE DEFAULTNUMINSTALLMENTS
        END as number_of_repayments,
        CASE
            WHEN MINNUMINSTALLMENTS IS NULL THEN 3
            ELSE MINNUMINSTALLMENTS
        END as min_number_of_repayments,
        CASE
            WHEN MAXNUMINSTALLMENTS IS NULL THEN 6
            ELSE MAXNUMINSTALLMENTS
        END as max_number_of_repayments,
        CASE
            WHEN ACCOUNTINGMETHOD = 'ACCRUAL' THEN 3
            ELSE 0
        END as accounting_type,
        pas.DEFAULTTOLERANCEPERIOD AS grace_on_arrears_ageing
    FROM {{ ref('loanproduct') }} as lp JOIN {{ ref('interestproductsettings') }} as ips
    ON lp.INTERESTRATESETTINGSKEY = ips.ENCODEDKEY
    JOIN {{ ref('productarrearssettings') }} as pas
    ON lp.ARREARSSETTINGSKEY = pas.ENCODEDKEY
)

SELECT
    ROW_NUMBER() OVER () as id,
    NULL as short_name,
    {{ decode_base64("currency_code") }} currency_code,
    cast(0 as int4) as currency_digits,
    cast(1 as int4) as currency_multiplesof,
    principal_amount,
    min_principal_amount,
    max_principal_amount,
    cast(NULL as numeric(19,6)) as arrearstolerance_amount,
    {{ decode_base64("name") }} name,
    description,
    cast(NULL as int8) as fund_id,
    false as is_linked_to_floating_interest_rates,
    false as allow_variabe_installments,
    nominal_interest_rate_per_period,
    min_nominal_interest_rate_per_period,
    max_nominal_interest_rate_per_period,
    cast(3 as int4) as interest_period_frequency_enum,
    nominal_interest_rate_per_period as annual_nominal_interest_rate,
    interest_method_enum,
    cast(0 as int4) as interest_calculated_in_period_enum,
    false as allow_partial_period_interest_calcualtion,
    repay_every,
    repayment_period_frequency_enum,
    number_of_repayments,
    min_number_of_repayments,
    max_number_of_repayments,
    cast(NULL as int4) as grace_on_principal_periods,
    cast(NULL as int4) as recurring_moratorium_principal_periods,
    cast(NULL as int4) as grace_on_interest_periods,
    cast(NULL as int4) as grace_interest_free_periods,
    cast(1 as int4) as amortization_method_enum,
    accounting_type,
    cast(1 as int4) as loan_transaction_strategy_id,
    {{ decode_base64("external_id") }} external_id,
    false as include_in_borrower_cycle,
    false as use_borrower_cycle,
    cast(NULL as date) as start_date,
    cast(NULL as date) as close_date,
    false as allow_multiple_disbursals,
    cast(NULL as int4) as max_disbursals,
    cast(NULL as numeric(19,6)) as max_outstanding_loan_balance,
    grace_on_arrears_ageing,
    cast(NULL as int4) as overdue_days_for_npa,
    cast(30 as int4) as days_in_month_enum,
    cast(360 as int4) as days_in_year_enum,
    false as interest_recalculation_enabled,
    cast(NULL as int4) as min_days_between_disbursal_and_first_repayment,
    false as hold_guarantee_funds,
    cast(0 as numeric(5,2)) as principal_threshold_for_last_installment,
    false as account_moves_out_of_npa_only_on_arrears_completion,
    true as can_define_fixed_emi_amount,
    cast(1 as numeric(19,6)) as instalment_amount_in_multiples_of,
    true as can_use_for_topup,
    false as sync_expected_with_disbursement_date,
    false as is_equal_amortization,
    cast(NULL as numeric(5,2)) as fixed_principal_percentage_per_installment,
    cast(NULL as int8) as product_category_id,
    cast(NULL as int8) as product_type_id,
    false as disallow_expected_disbursements,
    false as allow_approved_disbursed_amounts_over_applied,
    NULL as over_applied_calculation_type,
    cast(NULL as int4) as over_applied_number,
    false as is_loan_term_includes_topped_up_loan_term,
    cast(NULL as int4) as max_number_of_loan_extensions_allowed,
    true as is_account_level_arrears_tolerance_enable,
    false as is_bnpl_loan_product,
    false as requires_equity_contribution,
    cast(NULL as numeric(19,6)) as equity_contribution_loan_percentage
FROM source