# Lux Python Model Connector

Lux's connector implements durable Java generation-command acceptance, MySQL execution records, Kafka v2 observation delivery, and optional BytePlus Seedream/Seedance execution with S3 result preservation.

## Design

The [model connector design](docs/superpowers/specs/2026-10-10-model-connector-design.md) aligns with the Java/Python Session execution contract v2. Java owns business Tasks, Asset import, and terminal Session inputs. The connector owns provider submission, independent polling, preservation of original outputs, and immutable terminal observations.

A [Chinese translation](docs/superpowers/specs/2026-10-10-model-connector-design.zh-CN.md) is available.

## Requirements

- Python 3.14 or newer; `.python-version` selects the 3.14 series for version managers.
- `pip` and Python's built-in `venv` module.
- MySQL 8.0 or newer with a separately provisioned connector schema for enabled persistence.
- A Kafka broker for enabled command reception and Outbox delivery.
- A BytePlus API key and a private S3 or S3-compatible bucket when provider execution is explicitly enabled.
- FFmpeg (`ffprobe` on `PATH`) for enabled normalized result/reference inspection. Offline video-byte tests also use `ffmpeg`.

## Local setup

Run from the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

`pyproject.toml` pins FastAPI, Uvicorn, Pydantic, aiokafka, `pymysql[rsa]`, HTTPX, and boto3. The PyMySQL RSA extra supports normal MySQL 8 authentication. HTTPX implements the provider and media HTTP boundaries; boto3 supplies S3 authentication and object operations. The `dev` extra adds pytest and Ruff. Transitive dependencies are resolved by pip; there is no full dependency lock yet.

## Configuration and run

Without configuration, startup opens no MySQL or Kafka connection. `GET /health` remains a process liveness check and makes no database, broker, or provider calls.

| Environment variable | Meaning |
| --- | --- |
| `LUX_CONNECTOR_MYSQL_DSN` | `mysql://user:password@host:port/schema`; enables persistence in the provisioned connector schema |
| `LUX_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers; requires MySQL and enables the delivery worker |
| `LUX_CONNECTOR_KAFKA_GROUP_ID` | Command consumer group; default `lux-model-connector-v2` |
| `LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED` | Explicit `true` enables provider execution; defaults to `false`. Credentials alone never start generation |
| `LUX_BYTEPLUS_API_KEY` | BytePlus ModelArk bearer credential; required for enabled execution |
| `LUX_BYTEPLUS_ACCOUNT_REF` | Nonsecret persisted account reference; default `byteplus-primary` |
| `LUX_CONNECTOR_PROVIDER_POLL_SECONDS` | Fixed worker interval in seconds; default `5` |
| `LUX_CONNECTOR_S3_BUCKET` | Provisioned private result bucket; required for enabled execution |
| `LUX_CONNECTOR_S3_REGION` | S3 signing region; default `us-east-1` |
| `LUX_CONNECTOR_S3_ENDPOINT_URL` | Optional S3-compatible endpoint; omit for AWS S3 |
| `LUX_CONNECTOR_S3_ACCESS_KEY_ID`, `LUX_CONNECTOR_S3_SECRET_ACCESS_KEY` | Result-store credentials; required for enabled execution |
| `LUX_CONNECTOR_S3_SESSION_TOKEN` | Optional temporary-credential session token |
| `LUX_CONNECTOR_S3_PREFIX` | Immutable object-key prefix; default `connector-results` |
| `LUX_BACKEND_URL` | Optional Java backend base URL for owner-scoped reference Asset metadata/content; reference-bearing normalized requests fail validation when absent |
| `LUX_CONNECTOR_RESULT_BASE_URL` | Stable HTTP(S) base URL for the connector result gateway; required for enabled execution |

Keep credentials in private environment configuration. Percent-encode reserved characters in the DSN user/password. The optional `ssl_ca` query parameter accepts a percent-encoded CA file path and enables TLS certificate and hostname verification, for example:

```text
mysql://connector_user:encoded_password@localhost:3306/lux_connector?ssl_ca=%2Fabsolute%2Fpath%2Fca.pem
```

MySQL configuration without Kafka initializes the store but starts no delivery worker. Kafka without MySQL is rejected. Provider execution requires MySQL, the API key, S3 configuration, and a result base URL. Use a dedicated connector schema, separate from Java's schema; the application does not create databases or buckets. Keep its result base URL reachable by Java, and keep its bucket private. Startup configuration does not verify model activation or account permissions.

Start configured workers without automatic reloading:

```bash
python -m uvicorn lux_model_connector.main:app --port 8001
```

The server listens on `127.0.0.1:8001`, alongside `lux-python-agent` on port 8000. Use `--reload` for development with workers disabled.

- Health: <http://127.0.0.1:8001/health>
- Swagger UI: <http://127.0.0.1:8001/docs>
- ReDoc: <http://127.0.0.1:8001/redoc>
- OpenAPI schema: <http://127.0.0.1:8001/openapi.json>

`GET /health` returns HTTP 200 with `{"status":"ok"}`. It does not establish MySQL, Kafka, or provider readiness.

## Container runtime

The repository Dockerfile installs the existing package, including both schema SQL
files, in a build stage and runs it on `python:3.14-slim-trixie` with FFmpeg/ffprobe.
The build verifies their `fd` protocol support and Python dependency consistency.
The image runs as UID/GID `10001:10001`, binds `0.0.0.0:8001`, and starts one Uvicorn
worker without reload. Only package source, package metadata and this README enter
the build context; host virtual environments, credentials, Git state, tests and
local data are excluded. The floating base tag and unlocked transitive dependencies
do not provide a fully reproducible dependency snapshot.

Build and check the HTTP application from this repository:

```bash
docker build -t lux-python-model-connector:local .
docker run --rm --init -p 127.0.0.1:8001:8001 lux-python-model-connector:local
```

Without runtime configuration this exposes only the HTTP application. Use the
unified stack documented in
[`../lux-java-backend/deploy/local/README.md`](../lux-java-backend/deploy/local/README.md)
for isolated MySQL/Kafka/S3 and service DNS. Compose supplies private runtime
configuration; real provider execution remains disabled by default. MySQL needs a
dedicated provisioned Connector schema and the result bucket must already exist.
`LUX_CONNECTOR_RESULT_BASE_URL=http://model-connector:8001` gives Java a stable
in-network result gateway; container clients cannot use host `localhost` addresses.

The image creates `/var/lib/lux-model-connector` with mode `0700`, owned by UID/GID
10001, for an optional whole-directory data volume. Durable execution/results stay
in MySQL/S3; mounting this directory does not replace them. Existing or bind-mounted
directories must already grant UID/GID 10001 access. Supply credentials outside
the image and retain writable `/tmp` for media inspection and result spooling.
`/health` establishes process liveness, not database, broker or provider readiness.

## Persistence and execution boundary

Initialization accepts an empty schema or a recognized connector schema. `connector_schema_version` identifies the owner and bootstrap version; an interrupted recognized bootstrap can resume. Owned v1 schemas migrate additively to v2, adding execution claims and `generation_result`; the packaged v1 snapshot supports validation of the migration source. Foreign tables, unsupported versions, incompatible columns, SQL types, nullability, defaults, explicitly declared collations, auto-increment settings, required primary/unique indexes, or non-InnoDB tables cause startup rejection. Initialization does not replace an existing schema or create the database. MySQL DDL is not transactional, so migration steps are guarded and resumable.

The application owns `generation_execution`, `command_receipt`, `kafka_receipt`, `outbox_event`, and `generation_result`, plus the schema marker. Logical command identities and original physical Kafka key/value evidence are persisted with execution acceptance or a status-query response before offset acknowledgment. Conflicting or invalid inputs are diagnosed durably without altering the original accepted operation.

Admission uses named `ArchivedGenerationRequest` and `InputGenerationPayload` types to archive the original request before acknowledging Kafka. JSON archives use `LONGTEXT` with `JSON_VALID` checks where applicable, preserving numeric values rather than converting them through MySQL's native JSON representation. The enabled executor then decodes that archive into `SeedreamRequest` or `SeedanceRequest`. Invalid known input shapes produce a durable `VALIDATION` failure and terminal Outbox event. With execution disabled, submissions remain `ACCEPTED` with a null outcome.

The executor commits `RUNNING`, the exact provider/account/request snapshot, and a submission marker before one outbound POST. A timeout, connection loss, HTTP 408/5xx, malformed success response, or interrupted receipt persistence can leave acceptance uncertain. The execution remains `RUNNING`; an expired claim or missing external ID never authorizes another POST. There is no automatic resubmission. Default-disabled authenticated original-evidence attachment routes are described below. Receipt persistence retries in the same process; a lost, unrecorded synchronous response may require operational reconciliation.

Each worker independently scans advancing pages for original video IDs and saved image/video receipts. Queries have a 30-second deadline; submissions have a 600-second deadline. Query errors and unknown statuses alone do not produce `FAILED`. Submission and result preservation use separate local claims from read-only polling; their 15-minute database claims are renewed during result work. No polling ownership lease or per-execution reconcile timestamps are needed. `ConnectorStore.transition(...)` and `replay_terminal(...)` remain internal storage operations, not HTTP generation endpoints.

Short MySQL transactions run through `asyncio.to_thread` outside the event loop. Event sequence allocation and immutable Outbox insertion commit together. The publisher uses saved UTF-8 bytes, preserves per-Task order, and claims one due publication with a 30-second database-time lease. Failures retry with exponential delays from 2 to 60 seconds. Replay invalidates old claims so a late broker ACK cannot clear a newer replay request. Broker ACK does not prove Java imported the result.

## Normalized BytePlus generation

Enabled lifecycle registers these exact routes with profile `standard`, capability revision `1`, schema version `1`, provider `byteplus`, and the configured account reference (default `byteplus-primary`). Unknown or mismatched bindings fail before submission; there are no model aliases or fallbacks.

| Lux model | Provider model | Adapter | Outputs |
| --- | --- | --- | --- |
| `seedream-4.5` | `seedream-4-5-251128` | `byteplus-seedream-v1` | JPEG; square 2048 or 4096 pixels |
| `seedream-5.0-lite` | `seedream-5-0-260128` | `byteplus-seedream-v1` | PNG/JPEG; square 2048 or 4096 pixels |
| `seedream-5.0-pro` | `dola-seedream-5-0-pro-260628` | `byteplus-seedream-v1` | PNG/JPEG; square 1024, 1536 or 2048 pixels |
| `seedance-2.5` | `dreamina-seedance-2-5-260628` | `byteplus-seedance-v1` | MP4/MOV; 720p presets 1280x720 (16:9), 720x1280 (9:16), 960x960 (1:1); 4–30 seconds; audio enabled or disabled |

These finite presets are the initial Lux product subset. Images use explicit pixel dimensions. Seedream 4.5/Lite send `sequential_image_generation=disabled` and omit layer decomposition; Pro sends `layer_decomposition=false` and omits sequential controls. Seedance sends explicit `omni_reference_task_type=reference`, `draft=false`, and `return_last_frame=false`. Every normalized result requires one primary file with auxiliary outputs disabled.

Normalized prompts are limited to 4000 UTF-8 bytes. Reference Assets currently support only PNG/JPEG images. The Lux per-reference product caps are 3 MiB for 4.5/Lite, 4 MiB for Pro and 1.5 MiB for Seedance; the Java content transport retains a global 10 MiB ceiling. Seedance reference width/height must each be 300–6000 pixels, with width/height ratio 0.4–2.5. The count limits are 14 for 4.5/Lite, 10 for Pro and 30 for Seedance. Reference transport uses `/internal/v1/users/{user_id}/assets/{asset_id}` and its `/content` endpoint, validating identity, ownership, MIME type, bounded length, SHA-256 and actual media bytes before creating a provider-supported image data URI. The connector never resolves a private Java bucket directly or sends Java IDs/loopback download URLs to BytePlus. The product byte/count caps fit within the provider's 64 MiB body limit. Aggregate encoded references additionally reserve 1 MiB of the 64 MiB request/archive budget, and exact serialized provider requests are bounded independently. Video/audio references and the provider's separate portrait-reference workflow are not implemented. A reference containing a real human face may require that provider workflow; catalog support does not establish portrait compatibility.

The translated provider payload is saved before dispatch and reused on recovery. Seedance returns a typed accepted task ID; the worker durably stores it in the existing `provider_external_id` column before any polling. Terminal normalized receipts preserve typed original provider receipt metadata; image output base64 is archived once in primary outputs, with model/usage/timestamp/error metadata in the receipt. Older terminal normalized receipts remain readable without schema migration. A marked submission without an original ID/receipt stays uncertain; recovery never issues another POST. Polling only queries the saved original task ID.

Before normalized success, the connector reopens retained S3 bytes, verifies length/checksum, checks image/video format signatures and uses `ffprobe` frame counting to decode the media before inspecting dimensions and video duration/audio. Decoder errors or streams with no decoded frames fail verification. Requested duration tolerance is enforced against inspected bytes (Java's catalog uses 500 ms). This complements Java's independent Asset-import verification. Provider credentials/account permissions, real output, portrait references and Docker/MySQL/Kafka/S3 process integration remain separate verification gates; offline fixtures do not prove them.

## Legacy BytePlus requests and retained results

For legacy flat provider requests, upstream supplies a required, nonblank `model_id`. Any Model ID or Endpoint ID passes through unchanged to BytePlus's `model` field; the connector supplies no model default, allowlist, alias, or fallback. Model activation, permissions, and version-specific parameter capabilities are decided by BytePlus. The internal request body mirrors the documented flat API body, with `model_id` replacing `model` and named nested option/content types. Omitted options keep provider defaults; supplied parameters are never silently discarded.

For `kind=IMAGE`, an example `SUBMIT.request` is:

```json
{
  "model_id": "upstream-selected-model-or-endpoint-id",
  "prompt": "A cat sleeping in afternoon sunlight",
  "size": "2K",
  "watermark": false
}
```

For `kind=VIDEO`, an example `SUBMIT.request` is:

```json
{
  "model_id": "upstream-selected-model-or-endpoint-id",
  "content": [
    {"type": "text", "text": "The cat slowly opens its eyes"},
    {
      "type": "image_url",
      "image_url": {"url": "https://media.example.com/input.png"},
      "role": "first_frame"
    }
  ],
  "duration": 5,
  "generate_audio": false
}
```

The adapter implements the official [image generation API](https://docs.byteplus.com/en/docs/modelark/image-generation-api), [video task creation API](https://docs.byteplus.com/en/docs/modelark/create-video-generation-task-api), and [video task query API](https://docs.byteplus.com/en/docs/modelark/get-video-generation-task-api). Seedance content supports typed text, image, video, audio, and draft-task variants. Input media uses provider-supported URLs, data URIs, or BytePlus digital-character `asset://` URIs. Java's `asset_id`/`asset_ids` authorization conventions do not provide a content resolver here: unresolved Java Asset-ID fields fail request validation instead of being forwarded. Supplying accessible input media remains an upstream responsibility.

Image execution currently supports non-streaming responses. Omitted `stream` or explicit `false` works; explicit `true` produces `VALIDATION` failure. Permitting all Model IDs does not enable every transport feature. The full typed field definitions are in [byteplus.py](src/lux_model_connector/byteplus.py).

The connector checkpoints synchronous image responses or the original video ID, then preserves every required output in private S3 objects before `SUCCEEDED`. Partial image-generation errors and definitive provider rejection, failure, cancellation, or expiry produce `FAILED` with retained evidence. Provider URLs expire after 24 hours and are never published as the durable success manifest. Saved objects have stable keys by owner, Task, and source index; object metadata recovers a completed upload after a lost SQL checkpoint without downloading the provider URL again.

Result URLs use `${LUX_CONNECTOR_RESULT_BASE_URL}/internal/generation-results/{token}` with a random 256-bit capability token. The gateway streams the retained object with stable content metadata; it does not redirect to a presigned URL. Treat the token as a read credential and keep these URLs inside the trusted Java/connector boundary. Outbox replay keeps the same URL, source ordering, and outcome. Configure routing to reach this service and retention so objects remain available until the agreed import/cleanup policy; broker publication never deletes them automatically. Connector `SUCCEEDED` means retained output is available for Java import, which completes the business Task separately.

Provider response reads are bounded to 128 MiB and each retained media file to 100 MiB. Transfers and object operations have bounded timeouts. Transfer/storage uncertainty, including an original URL returning 404/410, keeps execution `RUNNING`: a delayed upload may still exist or appear at the immutable object key. Later scans check retained objects first. An expired URL with no confirmed retained output requires operational reconciliation, not another generation.

## Kafka and capacity

The consumer accepts only `lux.generation.commands.v2`; observations publish to `lux.generation.events.v2`. Keys are `user_id:task_id`. Automatic commits are disabled; each received offset is acknowledged only after its durable receipt. Persistence/commit failures retain the current record and do not skip to a later offset, while Outbox publication continues independently. Broker-confirmed ownership loss releases the record for redelivery under the new owner.

Complete serialized UTF-8 envelopes are limited to 31 MiB. The clients use 32 MiB request/record/partition-fetch capacity and 64 MiB aggregate fetch capacity, with an idempotent producer and `acks=all`. Configure broker/topic record limits for at least 32 MiB including record metadata, and MySQL `max_allowed_packet` for at least 64 MiB for ingestion-only operation. Enabled provider execution also archives large synchronous receipts: startup requires a packet limit of at least twice the response-byte budget plus 1 MiB (257 MiB for the default 128 MiB budget). Provision 512 MiB to allow SQL escaping overhead. The application checks this setting without modifying it. Oversized input is rejected; delivery never truncates a retained result or resubmits provider work.

## Verify

```bash
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m pip check
```

Protocol, provider HTTP, result-preservation, delivery, and default-health tests run without paid providers or cloud storage. MySQL integration tests and configured-lifespan tests skip unless `LUX_CONNECTOR_TEST_CONFIG` points to a private JSON file. Real Kafka tests also require its optional `kafka` value:

```json
{
  "host": "127.0.0.1",
  "port": 3307,
  "user": "connector_test_admin",
  "password": "replace-with-test-only-password",
  "kafka": "127.0.0.1:9092"
}
```

Use disposable, dedicated test servers, never production or application databases. The MySQL fixture creates a unique `lux_connector_test_*` schema and drops it in cleanup; the test account needs `CREATE DATABASE` and `DROP DATABASE` privileges. Kafka tests send generation commands to the v2 topics with fresh owners, Task IDs, and consumer groups. Keep the configuration outside Git with owner-only file permissions.

```bash
LUX_CONNECTOR_TEST_CONFIG=/absolute/path/connector-test-services.json python -m pytest
```

These checks exercise local protocol, provider fixtures, preservation recovery, persistence, lifecycle, and transport boundaries. They do not establish live Java/Agent integration, real model quality, account permissions, or cloud behavior. No paid provider or cloud operation is invoked by the offline test phase.

To build an installable wheel:

```bash
python -m pip wheel --no-deps --wheel-dir dist .
```

The wheel includes the owned `schema.sql` and v1 schema snapshot as package data so configured startup can initialize or migrate the same schema after installation.

## Structure

```text
src/lux_model_connector/
  __init__.py
  main.py          # Optional worker lifespan, result gateway, and process health
  protocol.py      # Typed v2 envelopes and opaque request archives
  store.py         # MySQL transactions, execution/result/publication claims
  schema.sql       # Owned, versioned InnoDB tables
  schema_v1.sql    # Validated migration-source snapshot
  delivery.py      # Kafka transport and recoverable delivery loop
  byteplus.py      # Typed Seedream/Seedance REST adapter
  execution.py     # Single submission and independent observation/recovery loop
  results.py       # Original-output preservation in private S3 objects
tests/
  conftest.py      # Opt-in isolated test services
  test_*.py        # Protocol, delivery, lifecycle, MySQL, and Kafka checks
pyproject.toml     # Package, dependencies, and tooling configuration
```

Provider execution is disabled by default. Live generation and cloud acceptance require separately agreed configuration and test scope; webhook reception and Java Asset-ID input resolution are not implemented.

### Normalized generation-plan requests

The connector explicitly decodes `image_generation_plan_v1` and
`video_generation_plan_v1` requests. Recognized malformed variants and unknown
bindings fail validation; they never fall through to legacy BytePlus decoding.
The production normalized adapter registry is empty. Legacy BytePlus support
continues to use its existing provider request and result semantics.

A normalized registration binds the exact provider/model/adapter, media type,
normalized schema version and model/profile/capability revision. Activation
requires an adapter that translates complete parameters, explicitly requests one
file with auxiliary outputs disabled, reconciles the original operation and
verifies preserved output against size/format/duration/audio expectations.
References require an authorized `AssetLocator` receiving the complete execution
identity; an Asset ID is never treated as a provider URL. No production locator
or normalized provider adapter is supplied by this change.

Before dispatch, the original normalized request, exact adapter binding, account
reference and translated provider archive are durably prepared in the existing
submission slot. Restart recovery uses that immutable slot. A marked original
without a receipt is reconciled through the same adapter and operation identity;
it is never submitted again. Normalized completion requires exactly one primary
file and adapter verification after immutable object preservation. Local test
adapters exercise the worker path without calling external services.

### Audited original receipt attachment

Operator reconciliation is disabled by default. Enable it only on a protected
internal network with `LUX_CONNECTOR_RECONCILIATION_ENABLED=true`,
`LUX_CONNECTOR_RECONCILIATION_TOKEN` (a secret Bearer token),
`LUX_CONNECTOR_RECONCILIATION_OPERATOR` (the trusted server-configured operator
identity), and `LUX_CONNECTOR_RECONCILIATION_ACCOUNTS` (comma-separated allowed
BytePlus account references). MySQL persistence is required. Every route requires
`Authorization: Bearer <token>`; the request cannot select its audit actor.

- `GET /internal/provider-reconciliation` returns up to 128 unresolved executions
  within the configured account scope.
- `GET /internal/provider-reconciliation/{user_id}/{task_id}` returns the original
  owner, Session, Task, operation, account, model, provider binding, request hash,
  and prepared submission hash. It omits original media and provider credentials.
- `POST /internal/provider-reconciliation/{user_id}/{task_id}/original-receipt`
  accepts the unchanged `binding` from the detail view, `evidence_reference`,
  `reason`, and either `external_id` for the original Seedance task or
  `image_receipt` containing the typed original Seedream response. The store
  validates all bindings and commits attachment and audit together.

Only marked, unresolved original submissions can acquire evidence. Existing IDs,
receipts, prepared submissions, and terminal outcomes cannot be replaced. Exact
same-evidence replay returns the saved view even after recovery completes; a
changed evidence payload is a conflict. The normal provider worker resumes the
original task GET or receipt preservation on its next polling cycle when provider
execution is enabled. Attachment creates no provider submission and does not
mark a Java business Task successful. Normalized image receipts continue through
the existing adapter output validation and durable result preservation.

The configured operator **attests** that the evidence belongs to the original
request. Authentication, exact stored bindings and audit hashes do not provide
cryptographic provider proof or automated provider idempotency. No provider API
for searching a missing task ID is assumed. If original evidence cannot be
recovered, keep the execution unresolved and do not submit it again.

Existing Connector-owned schemas upgrade additively to version 3 during startup;
no reset is required. To provision and validate explicitly using the existing
`LUX_CONNECTOR_MYSQL_DSN`, run:

```sh
python -m lux_model_connector.reconciliation --upgrade-schema
```

This adds the owned `provider_reconciliation_audit` table and retains previous
execution, receipt, result and Outbox records. Unsupported or foreign schemas
remain rejected. Agent receipt attachment uses a separate Agent-owned audit and
OpenAI response ID; neither service accepts evidence for the other service.
