# Task 15 — AML V2 Rule Improvement

## Objective
Reduce obvious false positives in the first AML rule set while preserving explainable,
point-in-time detection over the shared canonical transaction ledger.

## Changes
- Introduced rule version `aml_rules_v2`; V1 findings remain stored for comparison.
- FAN_IN and FAN_OUT now require at least four distinct counterparties within 60 minutes
  and aggregate transfer value of at least INR 100,000.
- STRUCTURING now requires at least four small outgoing transfers, at least three
  distinct recipients, and an aggregate of at least INR 100,000 in the 24-hour lookback.
- LAYERING now requires a prior incoming transfer within two hours and a forwarding
  amount between 80% and 105% of that incoming amount. A normal receive-then-send
  sequence alone is insufficient.
- CIRCULAR_TRANSFER now requires a strictly chronological prior path, bounded to
  four prior edges, and checks amount continuity across prior edges and the closing edge.
- When a network pattern is confirmed, findings are attached to all transactions
  participating in that confirmed pattern. The triggering event is recorded in evidence.
  This supports retrospective alerting of earlier edges after the pattern becomes visible.
- Added a safe inbox replay command. It re-evaluates stored AML inbox payloads using
  the active rule version, does not reset Kafka offsets, and does not delete V1 findings.
- AML evaluation now filters findings to the active rule version.

## Synthetic generator changes
- Normal hard-negative fan-in/fan-out motifs remain three-edge groups.
- AML fan-in/fan-out motifs use higher-value five-edge groups, making the intended
  distinction explicit in the synthetic benchmark.
- Structuring groups use repeated modest transfers whose aggregate crosses the rule threshold.
- Layering and circular-transfer groups preserve most of the value from one edge to
  the next, giving pass-through and cycle-continuity rules measurable evidence.

## Commands
From `backend`:

```powershell
python -m pytest
python -m app.generate_synthetic_data
python -m app.reprocess_aml_v2
python -m app.evaluate_aml_rules
```

Important: the live database is not automatically replaced by running the offline
synthetic generator. To evaluate the updated generator patterns, regenerate and load
a fresh controlled dataset using the project's documented seed/reset workflow. Do not
drop the current database until its data is backed up.

## Limitations
- Thresholds are demonstration parameters, not regulatory thresholds.
- Synthetic labels are generator-defined and do not establish real-world detection quality.
- The replay command uses the current canonical ledger and stored inbox payloads.
- V2 must be tested on the regenerated controlled dataset before claiming improved metrics.
