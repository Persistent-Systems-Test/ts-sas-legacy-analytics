-- Daily claims feed for the run date.
-- SAS: RAW_INS.CLAIMS_FEED_%sysfunc(putn("&proc_date"d, yymmddn8.))
with source as (
    select * from {{ source('raw_insurance', 'claims_feed') }}
)

select
    claim_id,
    policy_id,
    claimant_id,
    cast(loss_date as date)      as loss_date,
    cast(claimed_amount as number(18, 2)) as claimed_amount,
    cast(feed_date as date)      as feed_date
from source
where feed_date = to_date('{{ var("proc_date") }}')
