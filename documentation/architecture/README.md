# Sentinel Architecture Documentation

This folder records the approved system architecture and the reasons behind technology and design choices.

## Documents

- [System architecture](system-architecture.md) — components, responsibilities and end-to-end flow.
- [Technology decisions and alternatives](technology-decisions.md) — why each major component is included, alternatives considered and trade-offs.
- [Data flow and processing modes](data-flow.md) — real-time, near-real-time and batch responsibilities.
- [Local medallion data lake](medallion-data-lake.md) — Bronze, Silver and Gold datasets using local Parquet files.

## Decision status

These documents describe the **approved target architecture**, not a claim that every component has already been implemented. Implementation status must be tracked separately in the phase/task documentation. Proposed rates and schedules are starting configuration values and must be validated by measurements.

## Project constraints

Sentinel is an academic project simulating one bank. The design deliberately favors a working, testable end-to-end system over unnecessary infrastructure. Synthetic data is not evidence of real-world banking performance, fraud prevalence or model fairness.
