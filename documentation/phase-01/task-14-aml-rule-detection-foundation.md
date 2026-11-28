# Task 14 — AML Rule Detection Foundation

## Objective

Add an independent AML monitoring consumer to the shared banking event stream. The consumer uses canonical transaction history and explainable graph/velocity heuristics. It does not use synthetic AML labels or typology metadata as runtime inputs.

## Runtime flow

1. The AML consumer reads `bank.transactions.v1` using its own Kafka group.
2. The existing strict `TransactionCreatedEvent` contract validates the event.
3. The consumer verifies that the transaction matches the canonical PostgreSQL ledger.
4. Rules inspect completed transactions strictly earlier than the event timestamp.
5. Explainable findings are persisted with account references and evidence.
6. The inbox and findings are committed in PostgreSQL before the Kafka offset is committed.

## Initial rule set

- FAN_IN: at least three distinct senders transfer to one beneficiary within 60 minutes.
- FAN_OUT: one sender transfers to at least three distinct beneficiaries within 60 minutes.
- STRUCTURING: at least three transfers from one account in 24 hours, each no greater than INR 100,000, with aggregate at least INR 150,000.
- LAYERING: an account that received funds in the prior 24 hours forwards funds in the current event.
- CIRCULAR_TRANSFER: the current transfer closes a directed path of up to four prior hops within 24 hours.

The structuring values and time windows are configurable demonstration heuristics in code, not legal reporting thresholds, regulatory definitions, or validated bank policy. These simple rules produce leads for review, not conclusions that money laundering occurred.

## Persistence

- `aml_event_inbox`: durable event-id idempotency record.
- `aml_findings`: rule version, typology, subject account, related accounts, severity and structured evidence.
- The same canonical customers, accounts and transactions are reused; no second bank registry is created.

## Run locally

From `backend`, with PostgreSQL and Kafka running and the root `.env` loaded into the PowerShell process:

```powershell
python -m app.consume_aml
```

Set `KAFKA_AML_CONSUMER_GROUP_ID` to override the default group `aml-monitor-v1`, and `AML_CONSUMER_POLL_TIMEOUT_MS` to override the poll timeout.

## Limitations

- This is a rule-based academic prototype, not a production AML transaction monitoring system.
- The first pass uses a 24-hour lookback and bounded graph path search; it does not maintain a full graph database or perform customer risk scoring.
- No regulatory threshold is asserted.
- The synthetic labels are ground truth for offline evaluation only and are not read by the live rule engine.
- Automated tests use SQLite; PostgreSQL/Kafka runtime validation is still required.
- Findings are not automatically SAR/STR filings and require human investigation.
