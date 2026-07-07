-- Fraud alerts (HIGH risk) for SIU. SAS: DATA WORK.FRAUD_ALERTS -> STG_INS.FRAUD_ALERTS.
-- The SAS %sendmail SIU notification becomes an orchestration alert (see pyspark /
-- Databricks Workflows), not a transform.
select
    claim_id,
    policy_id,
    claimant_id,
    loss_date,
    claimed_amount,
    fraud_score,
    indicator_flags,
    'Fraud score: ' || cast(fraud_score as varchar)
        || '; ' || coalesce(indicator_flags, '') as alert_reason,
    to_date('{{ var("proc_date") }}') as alert_date
from {{ ref('int_claims_fraud_scored') }}
where fraud_risk = 'HIGH'
