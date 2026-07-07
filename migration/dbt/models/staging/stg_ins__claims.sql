-- Historical claims register. SAS: RAW_INS.CLAIMS
with source as (
    select * from {{ source('raw_insurance', 'claims') }}
)

select
    claim_id,
    policy_id,
    cast(loss_date as date)                 as loss_date,
    claim_status,
    cast(incurred_amount as number(18, 2))  as incurred_amount,
    cast(paid_amount as number(18, 2))      as paid_amount,
    cast(reserved_amount as number(18, 2))  as reserved_amount
from source
