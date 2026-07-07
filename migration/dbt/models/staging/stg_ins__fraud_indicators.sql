-- Fraud indicators. SAS: TERA_DW.FRAUD_INDICATORS
with source as (
    select * from {{ source('raw_dw', 'fraud_indicators') }}
)

select
    policy_id,
    claimant_id,
    cast(fraud_score as number(5, 0)) as fraud_score,
    indicator_flags
from source
