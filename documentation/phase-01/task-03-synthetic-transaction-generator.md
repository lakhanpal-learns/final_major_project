# Phase 01 — Task 03: Synthetic Transaction Generator

**Status:** Implemented in repository; local test verification pending  
**Date:** 2026-11-26

## Objective

Generate a canonical synthetic transaction ledger using the same Sentinel account registry used by both fraud and AML workflows.

## Implemented

- Added a typed Transaction model with canonical sender and receiver account IDs.
- Added deterministic generation of normal transfers.
- Added a separate fraud-labelled scenario using high-value unusual transfers.
- Added AML-labelled graph-pattern scenarios: structuring, fan-in, layering and circular transfer.
- Kept fraud labels and AML typology labels as separate fields.
- Ensured generated transfers use account IDs from the supplied canonical registry.
- Prevented self-transfers and generated unique sequential transaction IDs.
- Added seed-based reproducibility.
- Added tests for counts, labels, account references, self-transfer prevention, reproducibility and invalid inputs.

## Important interpretation

The fraud and AML labels are synthetic scenario ground truth created by the generator. They are not model predictions, and they do not demonstrate real-world fraud-detection accuracy. The AML patterns are simplified graph-shaped examples intended to provide known cases for pipeline development and evaluation. The notional amounts in the structuring examples are illustrative and are not legal or regulatory reporting thresholds.

Both fraud and AML events reference the same canonical Sentinel account IDs. No direct identity join between original PaySim and IBM AMLSim datasets is performed.

## Verification

- Source, tests and this document were committed to the main branch.
- Tests have not yet been executed locally for this task.
- Run python -m pytest from backend/ and review the output before treating Task 03 as verified.

## Known limitations / next work

- Transactions are generated in memory; they are not persisted to PostgreSQL.
- No API endpoint exposes the generator yet.
- No continuous event production, Kafka publishing or streaming consumer exists yet.
- Scenario generation is intentionally simplified and should be expanded with richer behavioral context and scenario-level grouping as the project develops.
