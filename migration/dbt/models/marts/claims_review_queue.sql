-- Manual review queue. SAS: PROC APPEND base=STG_INS.CLAIMS_REVIEW_QUEUE data=WORK.MANUAL_REVIEW.
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
    to_date('{{ var("proc_date") }}') as processing_date
from {{ ref('int_claims_adjudicated') }}
where is_manual_review
