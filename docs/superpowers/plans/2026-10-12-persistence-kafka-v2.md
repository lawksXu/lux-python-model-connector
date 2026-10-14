# Model Connector MySQL and Kafka v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Follow test-first verification and do not create Git branches or commits.

**Goal:** Durably accept Java generation commands, retain connector execution state, and deliver immutable v2 observations through a recoverable MySQL Outbox.

**Architecture:** A connector-owned MySQL schema holds executions, logical command receipts, physical Kafka receipts, and Outbox events. Short transactions establish identity, state, and event order; Kafka calls run outside them. ASGI lifespan enables the durable service only when configured; provider execution is outside this phase.

**Tech Stack:** Python 3.14, Pydantic 2.13.5, PyMySQL 1.1.2, aiokafka 0.14.0, FastAPI 0.141.1, MySQL 8.0 or newer.

## Global Constraints

- Follow the current connector design and Java `CommandPayload`/`EventProtocol` v2 contracts.
- Only `ACCEPTED`, `RUNNING`, `SUCCEEDED`, `FAILED` are execution states.
- Use `input_generation_payload`, `submission_payload`, `provider_external_id`, and `provider_receipt`; no execution request-hash column.
- IDs are canonical lowercase UUIDs; event sequences are positive signed 64-bit integers and query watermarks may be zero.
- Generation topics are `lux.generation.commands.v2` and `lux.generation.events.v2`; keys are `user_id:task_id`.
- Complete UTF-8 envelopes fit 31 MiB; Kafka record/request capacity is 32 MiB and aggregate fetch capacity 64 MiB.
- Terminal outcome/source/error validation matches Java: 1–64 sources, 255-character filenames, 128-character concrete media types, 64-character error codes, 65,535-byte error messages.
- Persist acceptance before offset commit. Terminal state, immutable outcome, original terminal event ID, sequence, and Outbox commit together.
- Retry and replay preserve event ID/time/sequence/payload. Publication claims prevent an old ACK from clearing a newer replay.
- Do not invoke providers or manufacture success. Received request bodies are explicitly typed opaque archives in this ingestion-only phase; selected model schemas are decoded when provider execution is added.
- MySQL must use a dedicated connector schema. Reject foreign or unsupported schemas before modifying tables. No Java/Agent source changes, production access, branch, commit, or instruction-file changes.

## Task 1: Typed v2 protocol

**Files:** `src/lux_model_connector/protocol.py`, `tests/test_protocol.py`.

**Interfaces:** Frozen named `GenerationSubmit`, `TaskStatusQuery`, `GenerationEvent`, `InputGenerationPayload`, `GenerationOutcome`, `GenerationError`, `ResultSource`, `ArchivedGenerationRequest`; `decode_command(raw: bytes | str)`, `encode_event(event: GenerationEvent) -> str`, `canonical_json(value: JsonValue) -> str`.

- [x] Write failing tests for v2 decoding, strict IDs/counters/timestamps, duplicate JSON keys, opaque archived request round trips, status/outcome agreement, source/error bounds, and UTF-8 transport budget.
- [x] Observe the expected failure before creating production types.
- [x] Implement explicit models and ingress decoding, preserving JSON scalar values and request omission/null semantics. Reject nonfinite numbers and malformed encoding.
- [x] Run `.venv/bin/python -m pytest tests/test_protocol.py` and `.venv/bin/python -m ruff check src/lux_model_connector/protocol.py tests/test_protocol.py`.

```python
assert decode_command(submit_json).request.model_dump() == original_request
assert encode_event(accepted_event).encode("utf-8")
with pytest.raises(ValueError):
    decode_command(submit_json.replace('"schema_version":2', '"schema_version":true'))
```

## Task 2: MySQL execution and delivery transactions

**Files:** `src/lux_model_connector/store.py`, `src/lux_model_connector/schema.sql`, `tests/test_store.py`, `tests/conftest.py`.

**Interfaces:** `MySQLSettings.from_dsn(dsn)`, `ConnectorStore(settings)`, `InboundRecord(topic, partition, offset, key, value)`, `ExecutionRecord`, `Publication`. Store methods: `initialize()`, `receive(record)`, `execution(user_id, task_id)`, `transition(user_id, task_id, status, outcome=None)`, `claim_publication()`, `publication_delivered(publication)`, `publication_failed(publication, error)`, `replay_terminal(user_id, task_id)`, `receipt_error(record)`.

- [x] Write MySQL integration tests before implementing the store: restart durability, operation/Task conflicts, duplicate event IDs, malformed diagnostics, database rollback, state regression, concurrent terminal commit, per-Task publication order, expired claim recovery, and stale ACK after replay.
- [x] Run the tests against an isolated test schema and confirm the missing-store failure.
- [x] Bootstrap only an empty or recognized connector schema, with a versioned initialization marker and recoverable owned DDL. Store raw ingress evidence separately from named business models.
- [x] Implement receive as one transaction: physical receipt + logical deduplication + execution acceptance/query response + Outbox. Invalid input is diagnosed before ACK; persistence errors propagate.
- [x] Implement transition with row locking and legal-state predicates. Compare identical terminal outcomes; reject conflicts. Allocate sequence and immutable event in the same transaction.
- [x] Claim only the earliest unpublished event per Task, using a fresh token/expiry. Broker ACK/failure updates require that claim; replay invalidates it.
- [x] Run `.venv/bin/python -m pytest tests/test_store.py` with the explicit isolated MySQL test DSN.

```python
store.receive(first_record)
store.receive(redelivered_record)
assert store.execution(owner, task).status == "ACCEPTED"
store.transition(owner, task, "RUNNING")
store.transition(owner, task, "SUCCEEDED", success)
original = store.claim_publication()
store.replay_terminal(owner, task)
assert not store.publication_delivered(original)
```

## Task 3: Kafka receipt and Outbox worker

**Files:** `src/lux_model_connector/delivery.py`, `tests/test_delivery.py`.

**Interfaces:** `GenerationTransport.poll/commit/publish/close`, concrete `KafkaTransport.start()`, `DeliveryWorker(store, transport).step()/run()`.

- [x] Write failing tests proving that receipt/offset failures retain the current record, later offsets cannot skip it, invalid commands commit only after durable diagnostics, and uncertain publication retries the same bytes.
- [x] Implement aiokafka with automatic commit disabled, one fetched record, required record/fetch budgets, idempotent producer, bounded startup/send/commit/close waits.
- [x] Run short synchronous MySQL operations outside the event loop. Retain pending records until successful commit or confirmed ownership loss; retry durable publication independently of a deferred command ACK.
- [x] Run `.venv/bin/python -m pytest tests/test_delivery.py` using real MySQL and a transport double, then a separate isolated live Kafka smoke test if the local environment permits it.

```python
await worker.step()
assert store.execution(owner, task) is not None
assert transport.committed == [record]
assert transport.published[0].envelope == original.envelope
```

## Task 4: Lifespan, dependencies, and operational documentation

**Files:** `src/lux_model_connector/main.py`, `pyproject.toml`, `README.md`, `tests/test_lifecycle.py`, design implementation-status paragraphs.

- [x] Write failing lifespan tests for disabled-by-default operation, Kafka without storage, configured workers, startup failure, and cancellation/cleanup.
- [x] Add pinned Pydantic, aiokafka, and MySQL driver dependencies; install only in the connector `.venv`. Include the driver's RSA support for normal MySQL 8 authentication.
- [x] Implement `ServiceSettings` and `create_app(settings, transport=None)`. Enable storage only through `LUX_CONNECTOR_MYSQL_DSN`, broker delivery through `LUX_KAFKA_BOOTSTRAP_SERVERS`, and group selection through `LUX_CONNECTOR_KAFKA_GROUP_ID`.
- [x] Document schema isolation, ingress-only ACCEPTED behavior, local testing commands, credentials/TLS configuration, schema/version safeguards, and the distinction between local checks and live Java/provider acceptance.
- [x] Run complete pytest, Ruff lint/format, dependency consistency, packaging, and the final spec/code review. Keep existing `/health` behavior as process liveness.

## Progress and Verification Ledger

- Baseline: one health test passes; repository has no HEAD, so work remains in the current directory without a branch or commit.
- Database decision: user selected MySQL with a schema separate from Java.
- Protocol: 144 strict validation cases pass; local Java URI/MediaType checks informed cross-language bounds.
- MySQL transactions: 27 cases pass against an isolated native MySQL 8.4.11 process, including rollback, restart, operation/event/offset concurrency, lifecycle timestamps, schema drift, ordering, and replay.
- Kafka worker: 13 transport/worker tests pass. A real Apache Kafka 4.1.2 round trip proves duplicate admission, committed offsets, restart, query response, RUNNING/SUCCEEDED publication, and exact persisted bytes.
- Lifespan: five cases pass, including disabled operation, mandatory storage, configured worker shutdown, and partial startup cleanup. README and paired implementation-status paragraphs document the new configuration.
- Verification: 191 tests pass with the private isolated-services configuration; Ruff lint passes. Wheel construction and schema/runtime inclusion pass; pip reports no broken requirements. Final Ruff lint/format, README/design JSON and local-link checks pass. The current wheel matches source/schema byte-for-byte. Independent code review found no remaining actionable issues; all earlier findings have regression coverage.

### Local Verification Boundary

Docker Desktop left all newly created test containers in `Created` before entrypoint execution. The tests therefore used official temporary MySQL 8.4.11 binaries and the Java files from `apache/kafka:4.1.2`, with independent data directories and localhost ports. The unused containers, anonymous volumes, and test network were removed. The temporary native test services were shut down after verification. Existing Java services, databases, providers, and deployed infrastructure were not used. Real provider execution, provider polling, result preservation, and Java Asset import remain later work.
