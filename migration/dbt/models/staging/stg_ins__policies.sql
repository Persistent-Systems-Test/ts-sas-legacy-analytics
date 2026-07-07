-- Policy master. SAS: RAW_INS.POLICIES
with source as (
    select * from {{ source('raw_insurance', 'policies') }}
)

select
    policy_id,
    customer_id,
    policy_type,
    upper(status)                          as status,
    cast(effective_date as date)           as effective_date,
    cast(expiration_date as date)          as expiration_date,
    cast(annual_premium as number(18, 2))  as annual_premium,
    cast(sum_insured as number(18, 2))     as sum_insured,
    cast(deductible as number(18, 2))      as deductible,
    risk_category,
    underwriting_class,
    agent_id,
    branch_code
from source
