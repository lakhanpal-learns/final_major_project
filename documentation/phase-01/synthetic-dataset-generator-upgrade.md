# Phase 01 — Synthetic Dataset Generator Upgrade

**Status:** Implementation committed; local execution pending

## Objective

Provide a larger, reproducible synthetic cohort for offline experiments without deleting or changing the operational PostgreSQL database. The export uses the existing canonical registry builder and transaction generator, so customer/account identifiers follow the same deterministic ID scheme used by Sentinel's fraud and AML pipelines.

## Default experiment size

| Dataset component | Default |
|---|---:|
| Customers | 1,000 |
| Accounts | 1,100 |
| Normal-only transactions | 20,000 |
| Fraud-positive transactions | 1,000 |
| AML-positive transactions | 1,000 |
| Total transactions | 22,000 |

Fraud and AML counts are positive-label counts. By default, one AML transaction is also fraud-labelled, so the four label combinations are not simply added together: 20,000 normal-only + 999 fraud-only + 999 AML-only + 1 fraud-and-AML = 21,999? **No**: because the generator's `normal_count` is normal-only and the fraud/AML positive counts include the overlap, the total is `normal_count + fraud_count + aml_count - overlap_count` = 21,999. The generator's current default overlap is one. Check the metadata for the actual total and class counts.

## Files

| File | Purpose |
|---|---|
| `backend/app/generate_synthetic_data.py` | Offline CLI, configuration, CSV export and metadata report. |
| `backend/tests/test_synthetic_dataset_export.py` | Checks export files, requested label counts and deterministic CSV output. |
| `backend/data/raw/synthetic/customers.csv` | Canonical synthetic customer registry. |
| `backend/data/raw/synthetic/accounts.csv` | Canonical synthetic account registry. |
| `backend/data/raw/synthetic/transactions.csv` | Mixed labelled transaction cohort with balance snapshots and scenario metadata. |
| `backend/data/raw/synthetic/metadata.json` | Parameters, counts, scenario coverage, time range and limitations. |

## Run

From the repository root:

```powershell
cd backend
python -m app.generate_synthetic_data
```

Default output is `backend/data/raw/synthetic/`. Override values before running:

```powershell
$env:SENTINEL_CUSTOMER_COUNT = "1000"
$env:SENTINEL_NORMAL_COUNT = "20000"
$env:SENTINEL_FRAUD_COUNT = "1000"
$env:SENTINEL_AML_COUNT = "1000"
$env:SENTINEL_REGISTRY_SEED = "42"
$env:SENTINEL_TRANSACTION_SEED = "99"
python -m app.generate_synthetic_data
```

Set `SENTINEL_SYNTHETIC_OUTPUT_DIR` to choose another output directory. This command writes local files only. It does not connect to PostgreSQL or Kafka and does not reset or append to the operational ledger.

## Important count correction

With default overlap of one, the expected total is **21,999**, not 22,000. To generate exactly 22,000 with the current label semantics, use `SENTINEL_FRAUD_COUNT=1001` or `SENTINEL_AML_COUNT=1001`, or explicitly change the overlap policy in a future revision. Do not silently misreport the total.

## Evidence and limitations

- The generator's labels are rule-generated synthetic ground truth, not verified real-world fraud findings.
- Scenario counts show template coverage, not realistic distribution or difficulty.
- Reproducibility tests establish deterministic file output for fixed seeds, not model quality.
- This export is an offline training cohort; Task 11 must be updated to read this export before it can prepare model features from this larger cohort.
- The current generator still models transfers only. It does not generate card purchases, cash withdrawals/deposits, salary credits, bill payments, merchant metadata, devices, IPs or login events.
- The larger cohort should be audited and split chronologically before training. Performance on synthetic test data does not establish production performance.
