-- Step 3: Auto-adjudication rules (ordered CASE preserves SAS if/return precedence).
-- SAS: DATA WORK.AUTO_ADJUDICATED / WORK.MANUAL_REVIEW.
with scored as (
    select * from {{ ref('int_claims_fraud_scored') }}
),

adjudicated as (
    select
        *,
        case
            -- Rule 1: high fraud -> deny (SIU referral), routed to manual review
            when fraud_risk = 'HIGH' then 'DENY'
            -- Rule 2: low risk, small claim, personal lines
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            -- Rule 3: low risk within limits
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            -- Rule 4: everything else pends for manual review
            else 'PEND'
        end as adjudication_result
    from scored
)

select
    *,
    case
        when adjudication_result = 'DENY'
            then 'High fraud risk - SIU referral'
        when adjudication_result = 'APPR' and claimed_amount <= 5000
            and policy_type in ('AUTO', 'HOME', 'RENT')
            then 'Auto-approved: low risk, small claim'
        when adjudication_result = 'APPR'
            then 'Auto-approved: within 25% of sum insured'
        else trim(
            case when fraud_risk = 'MEDIUM' then 'Medium fraud risk; ' else '' end
            || case when claimed_amount > 50000 then 'Large claim; ' else '' end
            || case when claimed_amount > sum_insured * 0.25
                    then 'Exceeds 25% threshold' else '' end
        )
    end as adjudication_reason,
    case
        when adjudication_result = 'DENY' then 0
        when adjudication_result = 'APPR'
            then greatest(0, claimed_amount - deductible)
        else null
    end as approved_amount,
    -- SAS routes DENY and PEND to the manual review queue; APPR is auto-adjudicated.
    adjudication_result in ('DENY', 'PEND') as is_manual_review
from adjudicated
