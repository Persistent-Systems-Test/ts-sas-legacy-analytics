-- Step 4: Claims register (append of auto-adjudicated + manual review).
-- SAS: PROC APPEND base=STG_INS.CLAIMS_REGISTER.
-- Incremental on (claim_id, processing_date) so re-running a proc_date is idempotent
-- (SAS PROC APPEND was not).
{{
    config(
        unique_key=['claim_id', 'processing_date']
    )
}}

with adjudicated as (
    select * from {{ ref('int_claims_adjudicated') }}
)

select
    claim_id,
    policy_id,
    claimant_id,
    loss_date,
    claimed_amount,
    policy_type,
    sum_insured,
    deductible,
    fraud_score,
    fraud_risk,
    adjudication_result,
    adjudication_reason,
    approved_amount,
    adjudication_result             as claim_status,
    to_date('{{ var("proc_date") }}') as processing_date
from adjudicated

{% if is_incremental() %}
where to_date('{{ var("proc_date") }}') not in (
    select distinct processing_date from {{ this }}
)
{% endif %}
