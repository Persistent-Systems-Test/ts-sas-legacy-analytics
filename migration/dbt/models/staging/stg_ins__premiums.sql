-- Premium collections. SAS: RAW_INS.PREMIUMS
with source as (
    select * from {{ source('raw_insurance', 'premiums') }}
)

select
    policy_id,
    cast(premium_amount as number(18, 2)) as premium_amount,
    cast(payment_date as date)            as payment_date,
    upper(payment_status)                 as payment_status
from source
