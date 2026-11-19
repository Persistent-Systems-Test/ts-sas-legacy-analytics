---
name: legacy-mapping
description: Trace legacy SAS banking and insurance requirements from batch orchestration through DATA-step business rules, exception outputs, and downstream consumers.
---

# Legacy Mapping — SAS legacy analytics

## When to use this

Use this repo-specific companion when validating business behavior against the migrated Java banking services. The cross-repo procedure and migration map are in `ts-java-spring-boot-internet-banking`. Use this SAS checkout as evidence for legacy behavior, not as proof that Java reproduces it.

## Layout

| Directory | Where to look |
| --- | --- |
| `Programs/Banking/` | Account snapshots and data-quality checks, daily transaction feeds, credit risk, and regulatory reporting. |
| `Programs/Insurance/` | Claims intake, validation, fraud screening, adjudication, and policy valuation. |
| `Programs/Reports/` | Customer profitability and report exports; some banking regulatory reports live under `Programs/Banking/`. |
| `Macro/` | Utility macros for parameter validation, row counts, dataset locks, hashes, exports, and notifications. Open the definition and the calling program to verify arguments and defaults. |
| `BatchJobs/` | Daily banking and insurance orchestrators. Read `%run_step`, run order, date gates and failure handling. |
| `Config/`, `Formats/` | Resolve librefs, autocall configuration and `PROC FORMAT` code-to-label mappings. |

## Trace a rule

1. Search case-insensitively for the business term, SAS variable names, exception codes and input/output datasets. Open the DATA or PROC step plus its wrapper and any relevant macro or format; check `%include` and macro call sites, even when include paths point outside this checkout.
2. Look for business rules in DATA-step `IF … THEN DO` blocks. A common account-quality convention is to set `EXCEPTION_CODE` and `EXCEPTION_DESC` and explicitly write to `WORK.*_EXCEPTIONS` (e.g. `NEG_BAL` → `WORK.ACCT_EXCEPTIONS`). Also check `REJECT_REASON` in `WORK.TXN_REJECTED`, `VALIDATION_ERROR` in `WORK.CLAIMS_INVALID`, PROC SQL `CASE`/`WHERE`, macro conditions, and derived flags. Not all rules use exceptions.
3. Determine whether a row is flagged and kept, routed away, or stops the job. Follow `OUTPUT`, `RETURN`, and later inserts/appends, including persistent staging/curated datasets, alerts and reporting jobs. Check SAS missing values, inclusivity, format vs. stored code, and batch reference date or run window when comparing with Java.
4. Always cite a repository-relative `file:line` or `file:start-end` from opened code for each rule and relevant consumer. Use `rg -in` to find candidates and `nl -ba` to verify line numbers.
