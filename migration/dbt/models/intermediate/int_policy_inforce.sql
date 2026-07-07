-- Step 1: In-force policies at valuation date + earned premium.
-- SAS: PROC SQL WORK.INFORCE. intck('month',..)->datediff('month',..);
-- intnx('year', d, 0, 'B')->date_trunc('year', d); intnx('month', d, 3)->dateadd.
{% set val_date = "to_date('" ~ var('valuation_date') ~ "')" %}
with policies as (
    select * from {{ ref('stg_ins__policies') }}
)

select
    policy_id,
    customer_id,
    policy_type,
    effective_date,
    expiration_date,
    annual_premium,
    sum_insured,
    deductible,
    risk_category,
    underwriting_class,
    agent_id,
    branch_code,
    datediff('month', effective_date, {{ val_date }})  as policy_age_months,
    datediff('month', {{ val_date }}, expiration_date)  as months_to_expiry,
    case
        when expiration_date <= dateadd('month', 3, {{ val_date }}) then 'Y'
        else 'N'
    end as renewal_due_flag,
    -- YTD earned premium: annual/12 * min(12, months earned this fiscal year)
    annual_premium / 12 * least(
        12,
        datediff(
            'month',
            greatest(effective_date, date_trunc('year', {{ val_date }})),
            least({{ val_date }}, expiration_date)
        )
    ) as ytd_earned_premium
from policies
where status = 'ACTIVE'
    and effective_date <= {{ val_date }}
    and expiration_date >= {{ val_date }}
    {% if var('lob') != 'ALL' %}
    and policy_type = '{{ var("lob") }}'
    {% endif %}
