-- Step 1: Ingest and validate claims against active policies.
-- SAS: DATA WORK.CLAIMS_VALID / WORK.CLAIMS_INVALID (hash lookup on POLICIES).
-- Produces one row per feed claim with an is_valid flag + validation_error so
-- downstream models can split valid vs invalid (mirrors the two DATA-step outputs).
with feed as (
    select * from {{ ref('stg_ins__claims_feed') }}
),

active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiration_date,
        sum_insured,
        deductible
    from {{ ref('stg_ins__policies') }}
    where status = 'ACTIVE'
),

joined as (
    select
        f.claim_id,
        f.policy_id,
        f.claimant_id,
        f.loss_date,
        f.claimed_amount,
        f.feed_date,
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.sum_insured,
        p.deductible,
        p.policy_id is not null as policy_matched
    from feed f
    left join active_policies p
        on f.policy_id = p.policy_id
)

select
    *,
    case
        when not policy_matched
            then 'Policy not found or inactive: ' || policy_id
        when loss_date < effective_date or loss_date > expiration_date
            then 'Loss date ' || to_char(loss_date, 'DDMONYYYY')
                 || ' outside policy period '
                 || to_char(effective_date, 'DDMONYYYY') || ' - '
                 || to_char(expiration_date, 'DDMONYYYY')
        when claimed_amount > sum_insured
            then 'Claimed amount ' || to_char(claimed_amount, '$999,999,999.00')
                 || ' exceeds sum insured '
                 || to_char(sum_insured, '$999,999,999.00')
        else null
    end as validation_error,
    (
        policy_matched
        and loss_date >= effective_date
        and loss_date <= expiration_date
        and claimed_amount <= sum_insured
    ) as is_valid
from joined
