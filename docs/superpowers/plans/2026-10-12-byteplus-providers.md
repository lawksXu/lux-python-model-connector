# BytePlus Seedream and Seedance Implementation Plan

> **For agentic workers:** Use subagent-driven development with test-first verification. Do not create branches, commits, or modify instruction files.

**Goal:** Execute upstream-selected image/video Model IDs through the official BytePlus ModelArk REST API, preserving the existing four-state and durable Kafka boundaries.

**Architecture:** Named request/response models map the connector-owned `model_id` to BytePlus `model`. Persist the exact submission before a single outbound POST; checkpoint synchronous image output or asynchronous video ID before further processing. Every worker independently scans bounded, advancing pages for provider status and receipt/result recovery. Preserve complete output before committing terminal state and its original Outbox event.

**Tech Stack:** Python 3.14, Pydantic, existing HTTPX promoted to runtime, PyMySQL, FastAPI, aiokafka. No provider SDK or live API call is required for offline acceptance.

## Decisions and contracts

- User supplied the official image/video create references and permits every upstream Model ID, including Endpoint IDs. Require a nonblank `model_id`; no model default, prefix rule, alias, routing fallback, or version allowlist.
- `kind=IMAGE` routes to `POST /images/generations`; `kind=VIDEO` routes to `POST /contents/generations/tasks`; video status uses `GET /contents/generations/tasks/{id}` under `https://ark.ap-southeast.bytepluses.com/api/v3`.
- Upstream typed API parameters mirror the supplied documented bodies except `model_id` maps to `model`. Validate known shapes and universal bounds without imposing model-version capability tables. Omit unspecified options so provider defaults remain authoritative. Never silently drop supplied parameters.
- All known content variants/options/response structures have named types. Original receipt JSON remains only archival; interpreted fields are typed.
- Never persist API keys or Authorization headers. Provider enablement requires explicit credentials plus result storage configuration. Disabled execution preserves the existing ingestion-only behavior.
- API submission has no verified idempotency/reconciliation promise. Persist its marker before sending, issue one POST, and never automatically resubmit after timeout, connection loss, malformed success response, ambiguous 5xx, or interrupted receipt persistence. Such executions remain `RUNNING`; independent video polling resumes only once its original external ID is known.
- A complete success needs all retained original outputs. Partial-image error, definitive provider failure/cancellation/expiry, and valid rejection yield typed failure evidence. Query/transport errors and missing original URLs alone are not generation failure.
- Provider URLs expire after 24 hours. The user selected S3/TOS-compatible object storage. Stable source-index keys and checked metadata recover uploads after a lost SQL checkpoint; expiring provider URLs are never published as connector success.
- Initial API reference adapters use non-streaming image results (`stream=false`); reject an explicit streaming request rather than discarding it. Streaming is a transport feature, not a restriction on Model IDs. Document this boundary.
- Public image/video URL/data inputs belong to the typed provider request. Java owns reserved Asset-ID references. Do not invent a Java input-resolution API or silently forward unresolved `asset_id` fields.

## Task 1 — Typed REST adapter

**Files:** `src/lux_model_connector/byteplus.py`, `tests/test_byteplus.py`, `pyproject.toml`.

- [x] Write failing tests for arbitrary Model IDs, all supplied typed body options/content variants, omitted-option preservation, errors, unknown video statuses, partial results, malformed IDs, synchronous receipt preservation, and no automatic POST retry.
- [x] Implement `BytePlusSettings`, upstream/provider request models, named response/receipt models, `BytePlusClient.generate_image/create_video/get_video`, with HTTPX injected at the actual HTTP boundary.
- [x] Bound timeouts/body sizes, authenticate only provider requests, keep download credentials separate, and emit errors without key/prompt contents.

```python
request = SeedreamRequest(model_id="endpoint-selected-by-upstream", prompt="cat")
assert request.provider_payload()["model"] == request.model_id
assert "model_id" not in request.provider_payload()
```

## Task 2 — Submission and receipt transactions

**Files:** `src/lux_model_connector/store.py`, `schema.sql`, `tests/test_provider_store.py`.

- [x] Write real-MySQL failing tests for one concurrent submission claim, pre-call marker, fixed request/account, late receipt acceptance, interrupted persistence, advancing scans, duplicate observations, and original terminal immutability.
- [x] Version the owned schema with an additive guarded migration; leave Java untouched. Add submission/result coordination separately from publication claims.
- [x] Prepare exact typed submission under an expiring claim; entry into RUNNING and its event commit together. A dispatch marker is conditional and durable before the external call. Expiry never authorizes repeating a marked call.
- [x] Persist external ID/receipt for the same immutable operation even after the network call outlives its local lease. Result recovery uses the receipt and cannot submit again.

```python
claim = store.claim_execution()
assert store.begin_submission(claim, prepared_payload)
assert not store.begin_submission(claim, prepared_payload)
```

## Task 3 — Provider execution/recovery loop

**Files:** `src/lux_model_connector/execution.py`, `tests/test_execution.py`.

- [x] Write failing tests for image/video progression, arbitrary Model IDs, known rejection, uncertain submit, saved receipt after crash, independent video scans, late/duplicate observations, all outputs preserved, and result failures without new generation.
- [x] Implement bounded independent worker cycles with cursor pagination, one active cycle per worker, and isolated per-task errors. No provider polling lease or per-execution reconcile timestamps.
- [x] Separate immutable submission/receipt from result-preservation claim and terminal transition; HTTP and file I/O remain outside MySQL transactions.

## Task 4 — Result preservation and lifespan

**Files:** `src/lux_model_connector/results.py`, `tests/test_results.py`, `tests/test_provider_lifecycle.py`, `main.py`, lifecycle tests, README and design status.

- [x] Resolve the user's storage choice before implementing dependent storage behavior.
- [x] Write failing tests for complete ordered manifests, recoverable transfer, immutable replay, and lifecycle startup/shutdown including absent credentials.
- [x] Configure provider HTTP clients/workers only when credentials/storage are present; expose the selected result access boundary with consistent immutable URLs.
- [x] Document request examples, Model ID pass-through, provider defaults, supported transport features, unresolved Asset access, schema migration, secret configuration, and uncertain submission recovery.

## Verification

- Baseline health/protocol/Kafka/persistence tests remain required.
- HTTP mock tests exercise official protocol without invoking a paid model.
- Real isolated MySQL tests prove transaction and race behavior; no existing application DB or provider is used.
- Run full pytest, Ruff lint/format, dependency check, wheel/schema inclusion, and independent code/spec review.
- API key and paid/live test scope remain separate; no live generation in this implementation turn.

## Implementation and evidence

- All Model and Endpoint IDs pass through the typed request; no version allowlist/default/fallback was introduced.
- The S3 adapter uses conditional immutable object writes and stable source-index lookup before downloading. The gateway serves retained original bytes through stable opaque read-capability URLs; private buckets need no public-read ACL.
- Fresh component verification: 65 HTTP API contract tests, 32 result-boundary tests, and 32 worker tests pass. Real MySQL tests cover dispatch exclusion, expired claims, late receipts, complete source cardinality, requested last frames, guarded v1 and interrupted v2 migration, and per-source result recovery.
- Integrated lifespan executes a mock image request through real MySQL and the SDK boundary double and serves the exact retained bytes. Header-send disconnect closes the opened object stream. Credentials alone leave generation disabled.
- Independent review findings were fixed: ambiguous missing-original/late-PUT remains RUNNING; provider startup checks packet capacity; gateway owns stream cleanup even before iteration.
- API key and object-store configuration remain pending from the user. No BytePlus generation or cloud object-store operation was executed. Paid provider acceptance, output quality, real S3/TOS compatibility, and Java Asset-import acceptance are separate live checks.

Final verification: 341 tests passed with isolated localhost MySQL/Kafka and mocked BytePlus/S3 boundaries. Ruff lint/format, dependency consistency, wheel construction, runtime/schema byte-for-byte inclusion, and documentation JSON/local-link checks passed. Independent review has no remaining findings. Temporary test services were shut down; no other services were changed.
