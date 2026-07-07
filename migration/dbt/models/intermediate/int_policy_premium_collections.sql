-- Step 3: YTD premium collections per policy.
-- SAS: PROC SQL WORK.PREMIUM_COLL. intnx('year', d, 0, 'B') -> date_trunc('year', d).
{% set val_date = "to_date('" ~ var('valuation_date') ~ "')" %}
with premiums as (
    select * from {{ ref('stg_ins__premiums') }}
)

select
    policy_id,
    sum(premium_amount)          as collected_premium,
    sum(case when payment_status = 'RETURNED'
             then premium_amount else 0 end) as returned_premium,
    max(payment_date)            as last_payment_date,
    count(case when payment_status = 'LATE' then 1 end) as late_payments
from premiums
where payment_date >= date_trunc('year', {{ val_date }})
    and payment_date <= {{ val_date }}
group by policy_id
