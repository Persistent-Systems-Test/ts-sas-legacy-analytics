-- Step 2: Fraud screening for valid claims.
-- SAS: PROC SQL WORK.FRAUD_CHECK (left join TERA_DW.FRAUD_INDICATORS) + FRAUD_RISK band.
with valid_claims as (
    select *
    from {{ ref('int_claims_validated') }}
    where is_valid
),

fraud as (
    select * from {{ ref('stg_ins__fraud_indicators') }}
)

select
    c.claim_id,
    c.policy_id,
    c.claimant_id,
    c.loss_date,
    c.claimed_amount,
    c.feed_date,
    c.policy_type,
    c.sum_insured,
    c.deductible,
    f.fraud_score,
    f.indicator_flags,
    case
        when f.fraud_score >= 80 then 'HIGH'
        when f.fraud_score >= 50 then 'MEDIUM'
        else 'LOW'
    end as fraud_risk
from valid_claims c
left join fraud f
    on c.policy_id = f.policy_id
    and c.claimant_id = f.claimant_id
