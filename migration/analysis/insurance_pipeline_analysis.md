# Insurance Pipeline — Migration Analysis

Migration analysis for the legacy SAS **insurance** analytics pipeline in this
repository. It documents the source system, extracts the embedded business
rules, maps the end-to-end data lineage, and defines the target design used by
the accompanying dbt (Snowflake) and PySpark (Databricks) implementations under
`migration/dbt/` and `migration/pyspark/`.

## 1. Scope

| Source SAS program | Cadence | Purpose |
|--------------------|---------|---------|
| `Programs/Insurance/claims_processing.sas` | Daily 08:00 (`INS_DAILY_01`) | Claims intake, validation, fraud screening, auto-adjudication |
| `Programs/Insurance/policy_valuation.sas` | Monthly, 5th business day (`INS_MONTHLY_01`) | In-force book valuation, loss ratios, IBNR / reserves |
| `Formats/insurance_formats.sas` | n/a | Domain value formats (policy type, claim status, risk, coverage) |
| `BatchJobs/run_daily_insurance.sas` | Daily 07:00 (`INS_MASTER`) | Control-M master orchestrator |

## 2. Source system

Per `Config/autoexec.sas`, SAS 9.4 M7 on RHEL 7. Relevant libraries:

| SAS libref | Physical / DB | Role | Target (this migration) |
|------------|---------------|------|--------------------------|
| `RAW_INS` | `/data/sas/raw/insurance` (read-only) | Landing zone (feed files, policies, claims, premiums) | Snowflake `RAW.INSURANCE` / Databricks `raw_insurance` |
| `TERA_DW` | Teradata `ANALYTICS` | Fraud indicators, actuarial tables | Migrated warehouse schema (`raw.fraud_indicators`) |
| `STG_INS` | `/data/sas/staging/insurance` | Persistent staging outputs | dbt marts / Delta tables |
| `REPORTS` | `/data/sas/reports` | Aggregated reporting outputs | dbt marts / Delta tables |
| `INSURANCE` | `/data/sas/formats/insurance` | Format catalog | dbt seeds / lookup tables |

## 3. Data lineage

### 3.1 Claims processing (daily)

```
RAW_INS.CLAIMS_FEED_YYYYMMDD ─┐
RAW_INS.POLICIES (ACTIVE) ────┼─► [validate] ─► CLAIMS_VALID ─┐
                              │                CLAIMS_INVALID  │
                              │                                │
TERA_DW.FRAUD_INDICATORS ─────┴──────────────► [fraud score] ─┼─► FRAUD_CHECK
                                                               │
                                        ┌──────────────────────┘
                                        ▼
                              [auto-adjudication rules]
                                        │
              ┌─────────────────────────┼───────────────────────────┐
              ▼                         ▼                            ▼
     AUTO_ADJUDICATED            MANUAL_REVIEW              FRAUD_ALERTS (HIGH)
              │                         │                            │
              └────────► STG_INS.CLAIMS_REGISTER                     │
                         STG_INS.CLAIMS_REVIEW_QUEUE ◄───────────────┘ (also)
                         STG_INS.FRAUD_ALERTS ◄───────────────────── (SIU email)
```

Column-level dependencies:

| Target field | Derivation |
|--------------|------------|
| `POLICY_TYPE`, `SUM_INSURED`, `DEDUCTIBLE`, `EFFECTIVE_DATE`, `EXPIRATION_DATE` | Hash join `CLAIMS_FEED.POLICY_ID → POLICIES` (STATUS='ACTIVE') |
| `FRAUD_SCORE`, `INDICATOR_FLAGS` | Left join `FRAUD_INDICATORS` on `(POLICY_ID, CLAIMANT_ID)` |
| `FRAUD_RISK` | `>=80 HIGH`, `>=50 MEDIUM`, else `LOW` |
| `ADJUDICATION_RESULT` / `CLAIM_STATUS` | Auto-adjudication rule set (§4.1) |
| `APPROVED_AMOUNT` | `max(0, CLAIMED_AMOUNT - DEDUCTIBLE)` for approvals; `0` for deny; null for pend |

### 3.2 Policy valuation (monthly)

```
RAW_INS.POLICIES (ACTIVE, in-force) ─► INFORCE ─┐
RAW_INS.CLAIMS (12-mo window) ─► CLAIMS_EXP ─────┼─► [merge by POLICY_ID] ─► STG_INS.POLICY_VALUATION
RAW_INS.PREMIUMS (YTD) ─► PREMIUM_COLL ──────────┘            │
                                                              ▼
                                        PROC MEANS by POLICY_TYPE
                                                              │
                                                              ▼
                                              REPORTS.LOSS_RATIO_SUMMARY
```

## 4. Business rules (extracted)

### 4.1 Claims validation (hard rejects → `CLAIMS_INVALID`)

1. **Policy must exist and be ACTIVE** — else `Policy not found or inactive`.
2. **Loss date within policy period** — `EFFECTIVE_DATE <= LOSS_DATE <= EXPIRATION_DATE`.
3. **Claimed amount <= sum insured** — else `Claimed amount exceeds sum insured`.

### 4.2 Fraud risk banding

| Band | Condition (`FRAUD_SCORE`) |
|------|---------------------------|
| `HIGH` | `>= 80` |
| `MEDIUM` | `>= 50` and `< 80` |
| `LOW` | `< 50` or no indicator record (null → LOW) |

### 4.3 Auto-adjudication (evaluated in order; first match wins)

| # | Condition | Result | Approved amount | Route |
|---|-----------|--------|-----------------|-------|
| 1 | `FRAUD_RISK = HIGH` | `DENY` | `0` | Manual review (SIU referral) |
| 2 | `FRAUD_RISK = LOW` AND `CLAIMED_AMOUNT <= 5000` AND `POLICY_TYPE in (AUTO, HOME, RENT)` | `APPR` | `max(0, CLAIMED - DEDUCTIBLE)` | Auto-adjudicated |
| 3 | `FRAUD_RISK = LOW` AND `CLAIMED_AMOUNT <= SUM_INSURED*0.25` AND `CLAIMED_AMOUNT <= 50000` | `APPR` | `max(0, CLAIMED - DEDUCTIBLE)` | Auto-adjudicated |
| 4 | otherwise | `PEND` | null | Manual review (reasons: medium fraud / large claim / exceeds 25%) |

> Rule ordering matters: rule 1 short-circuits before the approve rules, and rule 2
> before rule 3. The target implementations preserve this precedence with an
> ordered `CASE`.

### 4.4 Fraud alerting

- Rows with `FRAUD_RISK = HIGH` are written to `FRAUD_ALERTS` with
  `ALERT_REASON = 'Fraud score: <n>; <INDICATOR_FLAGS>'` and emailed to the
  on-call DL (SIU). In the target, the email side effect becomes an
  orchestration task (Databricks Workflows / dbt post-hook + alerting), not a
  transform.

### 4.5 Valuation metrics

| Metric | Formula |
|--------|---------|
| `POLICY_AGE_MONTHS` | `intck('month', EFFECTIVE_DATE, val_date)` |
| `MONTHS_TO_EXPIRY` | `intck('month', val_date, EXPIRATION_DATE)` |
| `RENEWAL_DUE_FLAG` | `Y` if `EXPIRATION_DATE <= val_date + 3 months` |
| `YTD_EARNED_PREMIUM` | `ANNUAL_PREMIUM/12 * min(12, months earned this FY)` |
| `LOSS_RATIO` | `TOTAL_INCURRED / YTD_EARNED_PREMIUM` (null if earned <= 0) |
| `COMBINED_RATIO` | `LOSS_RATIO + 0.30` (30% expense load) |
| `PREMIUM_ADEQUATE` | `N` if combined ratio null or `> 1.0`, else `Y` |
| `IBNR_ESTIMATE` | `max(0, YTD_EARNED_PREMIUM*0.15 - TOTAL_PAID)` |
| `TOTAL_RESERVE` | `OPEN_RESERVES + IBNR_ESTIMATE` |
| Open reserve claim statuses | `OPEN, INV, ADJ, PEND` |

## 5. SAS → target construct mapping

| SAS construct | dbt / Snowflake | PySpark / Databricks |
|---------------|-----------------|----------------------|
| `declare hash` lookup | `JOIN` in a CTE | broadcast `join` |
| `DATA` step multi-output (`CLAIMS_VALID`/`CLAIMS_INVALID`) | separate models filtering a shared validated model | `filter` into two DataFrames |
| ordered `if/return` adjudication | ordered `CASE` expression | chained `when().otherwise()` |
| `PROC SQL` join/aggregate | `SELECT ... GROUP BY` models | `groupBy().agg()` |
| `PROC APPEND ... force` | incremental model (`append`) | `write.mode("append")` / Delta MERGE |
| `PROC MEANS ... CLASS` | aggregate model | `groupBy().agg()` |
| `PROC FORMAT` catalogs | dbt seeds + join / `CASE` | broadcast lookup / `create_map` |
| `%macro`/`&macrovar` | Jinja + `var()` (`valuation_date`, `lob`) | function params |
| `intck/intnx` date math | `datediff`/`dateadd` | `months_between`/`add_months` |
| `%sendmail` SIU alert | post-hook / orchestration alert | Workflows task |

## 6. Parity & validation notes

- **Null fraud join → LOW**: SAS left join leaves `FRAUD_SCORE` null; the `CASE`
  falls through to `LOW`. Target `coalesce`s / relies on `ELSE 'LOW'`.
- **Earned premium**: uses fiscal-year-to-date proration; `min(12, ...)` caps a
  full year. Verify `intnx('year', date, 0, 'B')` = start of current year.
- **Idempotency**: SAS `PROC APPEND` is not idempotent per run; the dbt
  incremental models use `PROCESSING_DATE` / `VALUATION_DATE` as the natural key
  so re-runs of the same date do not double-count.
- Suggested reconciliation: row counts for
  `new / auto-adjudicated / manual review / fraud alerts` (SAS `%put` totals) vs
  the target model row counts for the same `proc_date`.
