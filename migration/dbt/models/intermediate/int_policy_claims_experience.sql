-- Step 2: 12-month claims experience per policy.
-- SAS: PROC SQL WORK.CLAIMS_EXP.
{% set val_date = "to_date('" ~ var('valuation_date') ~ "')" %}
with claims as (
    select * from {{ ref('stg_ins__claims') }}
)

select
    policy_id,
    count(distinct claim_id)     as num_claims,
    sum(incurred_amount)         as total_incurred,
    sum(paid_amount)             as total_paid,
    sum(reserved_amount)         as total_reserved,
    max(loss_date)               as last_claim_date,
    sum(case when claim_status in ('OPEN', 'INV', 'ADJ', 'PEND')
             then reserved_amount else 0 end) as open_reserves,
    sum(case when claim_status = 'DENY' then 1 else 0 end) as denied_claims
from claims
where loss_date >= dateadd('month', -12, {{ val_date }})
    and loss_date <= {{ val_date }}
group by policy_id
