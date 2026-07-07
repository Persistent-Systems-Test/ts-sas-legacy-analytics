-- Step 5: Loss ratio summary by line of business.
-- SAS: PROC MEANS CLASS POLICY_TYPE -> REPORTS.LOSS_RATIO_SUMMARY + calculated ratios.
with valuation as (
    select * from {{ ref('policy_valuation') }}
    where valuation_date = to_date('{{ var("valuation_date") }}')
),

summary as (
    select
        policy_type,
        count(*)                     as n_policies,
        sum(ytd_earned_premium)      as total_earned,
        sum(total_incurred)          as total_incurred,
        sum(total_paid)              as total_paid,
        sum(total_reserve)           as total_reserves,
        sum(ibnr_estimate)           as total_ibnr
    from valuation
    group by policy_type
)

select
    *,
    case when total_earned > 0 then total_incurred / total_earned else null end
        as agg_loss_ratio,
    case when total_earned > 0 then total_incurred / total_earned + 0.30 else null end
        as agg_combined_ratio
from summary
