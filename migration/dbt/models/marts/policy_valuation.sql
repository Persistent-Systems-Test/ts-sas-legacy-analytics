-- Step 4: Monthly policy valuation (merge in-force + claims exp + premiums; metrics).
-- SAS: DATA STG_INS.POLICY_VALUATION (merge by POLICY_ID, if a;).
-- Incremental on (policy_id, valuation_date) for idempotent monthly re-runs.
{{
    config(
        unique_key=['policy_id', 'valuation_date']
    )
}}

with inforce as (
    select * from {{ ref('int_policy_inforce') }}
),

claims_exp as (
    select * from {{ ref('int_policy_claims_experience') }}
),

premium_coll as (
    select * from {{ ref('int_policy_premium_collections') }}
),

joined as (
    select
        i.*,
        c.num_claims,
        c.total_incurred,
        c.total_paid,
        c.total_reserved,
        c.last_claim_date,
        c.open_reserves,
        c.denied_claims,
        p.collected_premium,
        p.returned_premium,
        p.last_payment_date,
        p.late_payments
    from inforce i
    left join claims_exp c on i.policy_id = c.policy_id
    left join premium_coll p on i.policy_id = p.policy_id
),

metrics as (
    select
        *,
        case when ytd_earned_premium > 0
             then coalesce(total_incurred, 0) / ytd_earned_premium
             else null end as loss_ratio
    from joined
),

final as (
    select
        *,
        case when loss_ratio is not null then loss_ratio + 0.30 else null end
            as combined_ratio
    from metrics
)

select
    *,
    case
        when combined_ratio is null then 'N'
        when combined_ratio > 1.0 then 'N'
        else 'Y'
    end as premium_adequate,
    greatest(0, ytd_earned_premium * 0.15 - coalesce(total_paid, 0))
        as ibnr_estimate,
    coalesce(open_reserves, 0)
        + greatest(0, ytd_earned_premium * 0.15 - coalesce(total_paid, 0))
        as total_reserve,
    to_date('{{ var("valuation_date") }}') as valuation_date
from final

{% if is_incremental() %}
where to_date('{{ var("valuation_date") }}') not in (
    select distinct valuation_date from {{ this }}
)
{% endif %}
