# Lux Model Connector Design

The model connector provides a typed internal protocol for external image and video generation services. It durably accepts generation requests, recovers interrupted execution, and reliably delivers execution outcomes to Java. Callers select the model; the connector validates the request contract and handles the provider integration.

The service boundaries and reliability direction were approved on 2026-10-10. On 2026-10-11, the execution lifecycle was simplified to four states, connector-owned execution persistence was provisionally agreed, and independent polling by each worker was selected for the initial version. On 2026-10-12, this design was aligned with the implemented local Java/Python Session execution contract v2, including generation envelopes, Task terminal inputs, and Java import acknowledgment/failure boundaries. The connector implements typed v2 ingestion, MySQL persistence, Kafka receipt/Outbox delivery, optional BytePlus Seedream/Seedance execution, independent polling, and original-output preservation in S3. Provider execution requires an explicit enable flag and complete credentials/storage configuration; live acceptance remains separate.

## Current cross-service baseline

The Java backend and Python agent have local v2 implementations. Their current contracts and source files, rather than the historical v1 plans, define the connector integration boundary:

| Reference | Relevant baseline |
| --- | --- |
| Java [Session execution contract v2](../../../../lux-java-backend/docs/api-contract-v2.md) | Current Task HTTP routes, flat generation commands, four-state callbacks, and import acknowledgment. |
| Java [generation commands](../../../../lux-java-backend/src/main/java/com/lux/backend/infrastructure/CommandPayload.java) and [generation decoder](../../../../lux-java-backend/src/main/java/com/lux/backend/infrastructure/EventProtocol.java) | Implemented v2 field names and separate legacy callback adaptation. |
| Python agent [README](../../../../lux-python-agent/README.md) and [Task HTTP boundary](../../../../lux-python-agent/src/lux_agent/backend.py) | Session execution, original operation reconciliation, immutable tool results, and explicit Task queries. |
| Java [Session storage design](../../../../lux-java-backend/docs/superpowers/specs/2026-10-09-session-run-turn-item-storage-design.md) | Business ownership, independent input Items, execution projections, and fresh v2 storage requirements. |

The local implementation baseline includes Java/Python v2 contracts and the connector's durable ingestion/delivery and provider execution paths. A valid envelope archives the original request as named `ArchivedGenerationRequest`, records `ACCEPTED` with a null outcome, and publishes its admission observation before Kafka acknowledgment. With `LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED=false` (the default), processing stops there. When enabled, the worker decodes the archive into `SeedreamRequest` or `SeedanceRequest`, records any validation failure durably, and executes the prepared original request. `ConnectorStore.transition(...)` and `replay_terminal(...)` are internal storage operations, not HTTP generation endpoints. Credentials alone do not enable generation.

The connector uses a separately provisioned MySQL schema and configuration-enabled Kafka workers; it does not create databases. Existing data, legacy consumers, and pending publications must follow the documented coordinated v2 cutover. Local protocol/store/transport checks and live Java/Agent/provider acceptance remain separate verification boundaries.

## Service responsibilities

| Component | Responsibility |
| --- | --- |
| Agent | Decide what to generate and construct the generation request. |
| Java backend | Own the business Task, its user and Session, input authorization, Asset import, and user-visible state. |
| Model connector | Validate the generation contract, adapt it to the selected provider, track execution, preserve results, and deliver execution outcomes. |

The connector correlates work through the business Task and submission operation. Its execution lifecycle does not depend on Run, Turn, Item, or tool-call lifetimes. Completion of a Run does not end background generation.

### Task submission and result consumption

1. Python calls Java `POST /internal/v2/users/{user_id}/tasks` with `operation_id`, `session_id`, `kind`, and `request`. For the current one-Task-per-call tool contract, `operation_id` is the original `TOOL_CALL` Item ID, not the model provider's `call_id`.
2. Java atomically creates the Session-owned Task and its v2 `SUBMIT` Outbox command, then returns the immutable `TaskSubmissionReceipt`: `type=task_submission_receipt`, `task_id`, and `accepted=true`. This confirms Java acceptance; connector admission and provider acceptance may still be pending.
3. The connector accepts that same operation, executes it independently of the originating Run, and reports progress or the final outcome to Java. The operation UUID is an idempotency identity, not a connector foreign key to an Agent Item.
4. Java turns definitive generation failure or completed Asset import into one Session input Item with `source=SYSTEM`, `kind=TASK_EVENT`, and content containing only `task_id`. Its `INPUT_ACCEPTED.event_id` equals that input Item's ID. Progress produces Task SSE invalidation without creating a terminal input.
5. Python durably admits the input and incorporates it at a subsequent frozen Turn boundary. When idle, a later admitted Task notification can wake a new Run under the Session stop/wake rules. It does not rewrite the original submission ToolResult or manufacture completion of the old Run.
6. To inspect the result, the model issues a new `get_task` call. Java `GET /internal/v2/users/{user_id}/sessions/{session_id}/tasks/{task_id}` returns `TaskQuerySnapshot`: `type=task_query_snapshot`, `task_id`, `version`, `status`, ordered `assets` with `asset_id` and `source_index`, and nullable `failure` with `stage`, `code`, and `message`. The failure stage is `GENERATION` or `IMPORT`; provider URLs are not the Agent result interface.

An uncertain Python submission is reconciled through Java `GET /internal/v2/users/{user_id}/tasks/by-operation/{operation_id}`, without repeating POST. A missing receipt does not prove no Task or external work exists. A confirmed `get_task` result remains immutable for its original call; another read is a new observation. Python may retain Run/Turn/tool-call bindings in its execution receipt to recover that call, but these are Python execution provenance, not business Task dependencies or connector fields. `STOP_SESSION` affects Agent Run execution, not the background generation Task.

Prompt planning, billing, Session scheduling, automatic model selection, provider fallback, user-initiated Task retries, and cancellation are outside this scope. Redelivery, status reconciliation, and delivery recovery preserve the original operation; they do not authorize another paid generation.

## Typed generation protocol

Image and video generation share identity and delivery conventions while retaining separate request types. The following are connector-owned logical types carried by, or persisted from, the current v2 envelopes. Concrete model request and options schemas still belong to the selected connector adapters.

| Type | Defined responsibilities and fields |
| --- | --- |
| `GenerationIdentity` | `user_id`, `session_id`, `task_id`, and `operation_id`; the first two preserve ownership and correlation without creating Run dependencies. |
| `SeedreamRequest` | Required upstream `model_id`, prompt or layer-decomposition input, image references, and all documented flat image options with named nested option types. |
| `SeedanceRequest` | Required upstream `model_id`, ordered named text/image/video/audio/draft content variants, and all documented flat video options. |
| `InputGenerationPayload` | Received generation request body: `identity`, `kind`, and the corresponding typed image/video `request`. A persistence snapshot, not a new wire envelope. |
| `ProviderSubmissionPayload` | Prepared provider call: `provider_id=byteplus`, nonsecret `account_ref`, `kind`, and the exact typed `request`. Fixed before calling the provider; contains no API key. |
| `ProviderMediaUrl` | Provider-supported URL, data URI, or BytePlus digital-character `asset://` URI. Java Asset-ID resolution remains a separate unimplemented input-access boundary. |
| `ResultSource` | Download `url`, `filename`, and `media_type`, preserving the existing Java result-import boundary. |
| `GenerationError` | `code`, `stage`, `message`, and optional named `provider_diagnostic`; defined below. |
| `GenerationOutcome` | Discriminated union of `GenerationSuccessOutcome` and `GenerationFailureOutcome`; defined below. |

The BytePlus request types mirror the supplied official API bodies, except upstream `model_id` maps exactly to provider `model`. Every nonblank Model ID or Endpoint ID is permitted; there is no allowlist, prefix rule, default model, alias, or automatic fallback. Local validation checks known shapes and universal bounds. Model activation, permissions, and version-specific capability combinations remain provider decisions. Omitted options retain provider defaults, explicit false values are preserved, and supplied null or unknown fields are rejected instead of silently discarded.

Known business structures use named Pydantic models, dataclasses, or equivalent explicit types. A generic dictionary must not replace a known request, options object, result, or error. Raw provider data may remain opaque only when the connector archives or forwards it without interpreting its fields.

Java validates reserved input Asset ownership. The connector validates its known request shape before submission and passes the selected Model ID to BytePlus unchanged. The implemented input boundary accepts provider-supported media references; it does not resolve Java Asset IDs or invent a Java content-access API.

Java and Python wrap connector-owned parameters in `ConnectorRequest`; they do not own concrete provider options. Python currently interprets top-level `asset_ids`, while Java validates reserved `asset_id`/`asset_ids` references throughout the request. Those conventions do not provide the connector with authorized file access. Unresolved Java Asset-ID fields are rejected by the typed BytePlus adapter; upstream must currently provide accessible media references. `input_generation_payload` is a storage snapshot derived from the flat submission; it is not a nested wire member or a replacement for `request`.

### BytePlus adapter boundary

`kind=IMAGE` selects `POST /images/generations`; `kind=VIDEO` selects `POST /contents/generations/tasks`, with status read through `GET /contents/generations/tasks/{id}`. All calls use the fixed official base URL `https://ark.ap-southeast.bytepluses.com/api/v3`. The [image reference](https://docs.byteplus.com/en/docs/modelark/image-generation-api) and [video create reference](https://docs.byteplus.com/en/docs/modelark/create-video-generation-task-api) supplied by the user define the request shapes; the [video query reference](https://docs.byteplus.com/en/docs/modelark/get-video-generation-task-api) defines status receipts. Named response models preserve interpreted output, layer metadata, usage, and provider errors; future unknown response metadata is ignored.

Image execution accepts non-streaming responses only: omitted `stream` or explicit `false` is supported; explicit `true` fails local validation. This is a transport boundary independent of permitting every Model ID. Video input supports text, image, video, audio, and draft-task variants. Version-specific combinations are not reimplemented as local model tables. `callback_url` can be forwarded as a documented request option, but the connector itself has no BytePlus webhook receiver; its recovery path polls the original task.

The exact submission and possible-dispatch marker commit before one POST. Synchronous image output is checkpointed as `SeedreamReceipt`; video creation checkpoints its external ID, with completion checkpointed as `SeedanceReceipt`. No verified BytePlus idempotency or lookup-by-original-request guarantee is assumed. HTTP 408/5xx, timeout, connection loss, malformed success data, or lost receipt persistence leave the execution `RUNNING`; neither an empty external ID nor claim expiry permits automatic resubmission. An unresolved original submission requires operational reconciliation, with no manual reconciliation HTTP endpoint added here. Valid 4xx rejection, a partial image error, or video failure/cancellation/expiry gives definitive failure evidence; a query transport error or unknown status does not.

Execution is enabled only by `LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED=true`, with MySQL, API credentials, S3 configuration, and a stable connector result base URL. Request examples and exact environment variables are maintained in the [README](../../../README.md). HTTPX supplies the REST boundary; boto3 supplies signed S3 operations. Offline provider fixtures do not prove live account/model availability.

### Terminal outcome structure

The `outcome` column can store a JSON object, but it represents a defined business type rather than an arbitrary dictionary. Python processing must use the corresponding named models. Nonterminal executions have no outcome. A terminal outcome's `status` discriminator must match the execution status, and the snapshot is immutable after its terminal commit.

| Type | Exact fields and constraints |
| --- | --- |
| `GenerationSuccessOutcome` | `status`: literal `SUCCEEDED`; `sources`: ordered list of 1 to 64 `ResultSource` values, containing the complete required manifest. No error field. |
| `GenerationFailureOutcome` | `status`: literal `FAILED`; `error`: required `GenerationError`. No sources field. |
| `ResultSource` | Required strings `url`, `filename`, and `media_type`. URL is complete HTTP(S) with a host and no user information; filename is nonblank, at most 255 characters, with no path separator or control character; media type is nonblank, at most 128 characters, and concrete rather than wildcard. Availability follows the result-preservation contract below. |
| `GenerationError` | Required nonblank strings `code` and `message`; code is at most 64 characters and message at most 65,535 UTF-8 bytes. `stage`: one of `VALIDATION`, `PROVIDER`, or `RESULT_PRESERVATION`; optional `provider_diagnostic`: `ProviderErrorDiagnostic`. These are error categories, not execution stages or additional task states. |
| `ProviderErrorDiagnostic` | Required `provider_id`; optional strings `provider_code` and `provider_message`. No credentials or undefined business fields. |

GenerationError.code is a connector-owned classification and must fit the current Java Task.error_code VARCHAR(64). Validate code and message bounds without silent truncation. Preserve original provider evidence in the connector receipt and named provider diagnostic; a native provider code is not shortened into the connector classification.

Example persisted success:

```json
{
  "status": "SUCCEEDED",
  "sources": [
    {
      "url": "https://media.example.com/result.png",
      "filename": "result.png",
      "media_type": "image/png"
    }
  ]
}
```

Example persisted failure:

```json
{
  "status": "FAILED",
  "error": {
    "code": "PROVIDER_REJECTED",
    "stage": "PROVIDER",
    "message": "The provider rejected this request."
  }
}
```

These objects specify both the connector's stored outcome and the terminal `GenerationEvent.outcome` accepted by Java v2. Legacy v1 adaptation remains separate, and connector success still precedes Java Asset-import success. Validate result-count, metadata, and error limits before publishing; a model's allowed output count must fit the receiving contract before paid submission.

## Internal delivery

The current Java generation path uses schema version 2 and these topics:

- `lux.generation.commands.v2` for submission and status queries.
- `lux.generation.events.v2` for connector observations and execution outcomes.

Java also receives `lux.generation.events.v1` through a separate legacy adapter. New connector commands and events use v2; the legacy receive path is not a new v1 submission path. Topic and schema version must agree. Consumer activation must isolate legacy workers, offsets, and pending publications so old backlog is not interpreted as v2 input.

The generation partition key remains `user_id:task_id`. Events retain stable event IDs and monotonically increasing sequence numbers per Task. Publication retries reuse the original envelope, ID, sequence, and payload, and the publisher preserves event order per Task. Exact duplicates are harmless; an existing identity with different content is a recorded conflict. Java Task versions are separate business counters, not connector event sequences.

The internal callback is a generation event delivered through Kafka. A provider webhook is a separate ingress boundary owned by its provider adapter. HTTP submission and caller-specific HTTP callbacks are not added to the initial delivery path.

### V2 generation envelopes

All generation envelopes are flat objects with `schema_version=2`, stable `event_id`, `event_type`, original `occurred_at`, `user_id`, and `session_id`. IDs are canonical lowercase UUIDs and timestamps are UTC at microsecond precision. The command or observation adds these members at the top level:

| Named envelope | Additional members and meaning |
| --- | --- |
| `GenerationSubmit`, `event_type=SUBMIT` | `task_id`, `operation_id`, `kind` (`IMAGE` or `VIDEO`), and connector-owned `request`. No Run or tool-call reference. |
| `TaskStatusQuery`, `event_type=TASK_STATUS_QUERY` | `task_id` and nonnegative signed 64-bit `last_observed_sequence_no`, Java's current connector-observation watermark. No operation or provider ID is required in the query. |
| `GenerationEvent`, `event_type=GENERATION_EVENT` | `task_id`, positive signed 64-bit `sequence_no`, `connector_status: ConnectorExecutionStatus`, and `outcome: GenerationOutcome | null`. Emit this common observation form for progress, terminal outcomes, and explicit status responses. |

Java's v2 decoder accepts the named status/outcome structure; it does not dispatch on the five historical v1 event names. `ConnectorExecutionStatus` is exactly `ACCEPTED`, `RUNNING`, `SUCCEEDED`, and `FAILED`. `ACCEPTED`/`RUNNING` carry explicit null outcome; `SUCCEEDED`/`FAILED` require the matching outcome variant. Provider external IDs, native receipts, `run_id`, `turn_id`, and `tool_call_id` are not added to the v2 generation envelope. The connector resolves the provider operation from its own persisted execution using the owner/Session/Task identity.

| Connector observation | Java business behavior |
| --- | --- |
| `ACCEPTED` | Task advances from `CREATED` to `SUBMITTED`, meaning durable connector admission. |
| `RUNNING` | Task advances to `RUNNING`; connector processing includes submission, reconciliation, and result preservation. |
| `SUCCEEDED` with complete manifest | Task enters `IMPORTING`; only durable validated Assets permit Java `SUCCEEDED`. |
| `FAILED` with defined error | Task enters `FAILED` with `error_stage=GENERATION` and the original error code/message. |

The per-Task `sequence_no` maps to Java Task's `provider_event_seq`; Java Task `version` remains its independent business snapshot version. Session input `input_sequence_no`, Python execution-fact `event_seq` and `source_revision`, and Java SSE sequence are separate counters. Connector observations are not Agent execution snapshots and do not carry Python checkpoint or Run/Turn revision fields. The finer generation error stage and optional provider diagnostic remain in Java's original Inbox envelope rather than becoming extra Task fields.

An explicit status query is deduplicated by its command event ID and observes the existing execution. A new response uses the connector's own next sequence and the same immutable terminal outcome where present; replaying the original terminal event preserves its original ID/sequence/content. The caller's watermark is not permission to recreate an execution or reset its counter. Query failure, inconclusive missing execution, or uncertain submission cannot fabricate `FAILED`, `ACCEPTED`, or successful result data. Java's observation scanner only requests connector state for generation-stage Tasks; it neither queries suppliers nor downloads results. It remains independent of the connector's per-worker supplier polling loops.

Provider-to-connector webhooks remain a separate ingress direction, selected per provider. Synchronous API responses and polling responses are observations, not webhook callbacks. Java's Task SSE invalidation and terminal `INPUT_ACCEPTED` delivery to the Agent are separate business notifications.

### Transport and acknowledgment

The implemented Java/agent v2 baseline permits a 31 MiB complete UTF-8 JSON envelope, with 32 MiB Kafka request/record/partition-fetch capacity and 64 MiB aggregate fetch capacity. The connector must use compatible broker/client limits and measure its complete serialized envelope. Oversized Java submissions are rejected before Task/Outbox acceptance commits. An already executed provider result must retain its full outcome and delivery evidence if publication exceeds the budget; do not truncate it or repeat generation. Repair the transport/schema mismatch before delivery proceeds.

MySQL ingestion-only operation requires `max_allowed_packet` of at least 64 MiB. Provider-enabled startup checks a separate receipt capacity: at least twice the provider response-byte budget plus 1 MiB, which is 257 MiB with the default 128 MiB response budget; provision 512 MiB for SQL escaping overhead. The connector rejects insufficient capacity before starting paid workers and does not change the server setting. The Kafka outcome contains stable retained-result URLs, not base64 image bytes.

Kafka publication acknowledgment proves broker acceptance, not Java application or Asset import. Java saves original generation Inbox evidence without acknowledging a result callback, downloads outside database transactions, then commits the Task terminal outcome, unique Session input, SSE, Outbox, and Inbox `APPLIED` before acknowledging the completed partition prefix. Technical interruption resumes the same event and accepted manifest through Kafka redelivery; no callback Inbox scanner or MySQL named lock is used. Current Java handling processes one polled record at a time for this acknowledgment boundary. Agent/Python receipt-before-ack and checkpoint recovery remain independent mechanisms.

## Durable acceptance and execution

The connector durably records command acceptance, execution, and publication. For a valid submission, the execution record may also serve as its acceptance and deduplication record; a separate Inbox table is not mandatory. If a separate receipt is used, receipt and execution creation commit in one local transaction before the Kafka offset is acknowledged. A recorded invalid or conflicting command must remain diagnosable without repeatedly blocking unrelated work. Failure to persist acceptance leaves the command unacknowledged.

Before Kafka acknowledgment, admission persists the envelope, identity, immutable request archive, and `ACCEPTED` execution. The enabled worker then decodes and validates the named provider request asynchronously. Invalid known shapes produce a failed execution and terminal failure Outbox event in one transaction before that work is considered complete, without a provider call. The durable accepted row makes validation recoverable after the command was acknowledged. Model availability or version-specific capability rejection is instead established by the provider response. Malformed identities, unauthorized submissions, and conflicting duplicates are diagnostic rejections; they must not replace or fail the original accepted operation.

The submission identity is scoped by `user_id` and `operation_id`, matching Java's immutable Task submission. The accepted Task identity and request payload are fixed for that operation. A repeat with the same identity and content returns or observes the existing execution. Compare normalized request content directly; a separately persisted request hash is not required. JSON object key order and transport event IDs or timestamps do not change request identity. Normalization preserves ordered input lists and the protocol's omitted/null/default semantics. A repeat with a different Task, Session, model, or request is a conflict. Duplicate transport events do not create duplicate executions. Do not reconstruct the request from a current Agent checkpoint or require the originating Run to remain active.

Provider submission and result preservation retain their own ownership or idempotency protections. Where a database lease is used, a claim or revision check prevents stale writes to that claimed work; a lease alone cannot make provider submission idempotent. In the initial version, each worker independently polls queryable executions without claiming an execution lease. Applying polling observations uses conditional database updates and the terminal-commit rules below.

Before a provider call, persist the submission payload, its start marker, and the provider correlation or idempotency key that will be used. This payload is a snapshot of what the connector plans to send, fixing the provider, account reference, and exact typed provider request so recovery does not reinterpret an old execution using changed model configuration. It is created before the call, whereas a provider external ID or response is obtained after the call. Account references contain no credentials. Persist the accepted provider external ID immediately when it is available. Keep additional response evidence only when recovery needs more than that ID. Restart recovery uses these records to query or resume observation of the existing provider execution.

### Execution states

These are the only values of `generation_execution.status`, the connector's task execution status. Java's business Task and its Asset-import lifecycle remain separate. Provider submission, reconciliation, and result preparation are processing details represented by durable facts, not another execution-stage enum.

| State | Meaning |
| --- | --- |
| `ACCEPTED` | Request is durable and the connector has not started processing it. |
| `RUNNING` | The connector has started processing but has not reached a terminal outcome; includes submission, provider waiting or execution, reconciliation, and result preparation. |
| `SUCCEEDED` | The complete recoverable result manifest is durable and its sources are available for Java import. |
| `FAILED` | A definitive execution failure is recorded. |

Normal transitions are `ACCEPTED` to `RUNNING`, then `RUNNING` to `SUCCEEDED` or `FAILED`. A trusted submission rejected during validation may go directly from `ACCEPTED` to `FAILED`, or be durably created as `FAILED` together with its failure event. No provider call is made in that case.

`SUCCEEDED` and `FAILED` are immutable execution terminal states. Old progress cannot replace a terminal outcome. A conflicting late observation is retained for diagnosis without producing a different terminal outcome.

A task in `RUNNING` does not assert that the provider is currently generating. Submission uncertainty and incomplete result preservation remain `RUNNING`; a provider completion observation alone is insufficient for connector success.

The connector commits `RUNNING` before processing can produce an external side effect. Every provider submission also requires a previously committed `submission_started_at` and fixed submission payload. Under that invariant, a recovered execution without the marker can continue preparation and make its first submission while holding a valid lease. A marker with no confirmed receipt means a request may have been accepted; a crash before sending and a crash after acceptance can leave the same record. Use provider idempotency or lookup support to resolve the gap. A missing `provider_external_id` is not evidence that submission never happened; synchronous providers may not issue such an ID.

An uncertain submission stays `RUNNING`. A timeout or exhausted reconciliation budget requires attention while retaining the uncertainty. If a provider offers neither safe idempotent submission nor a way to find an accepted operation, the connector cannot guarantee both eventual execution and absence of duplicate paid work. Provider onboarding must establish the supported recovery behavior.

Polling, local recovery, and message publication may use bounded attempts and backoff. Exhausting those attempts does not by itself establish a provider execution failure. No terminal failure is reopened for another generation.

## Execution persistence

### Data ownership

The connector owns lightweight generation execution records and event publication records. Java remains the sole owner of its business Task, Session, authorization, and Asset tables. The connector reuses Java's `task_id` for correlation and does not create a second business Task identity, copy the business schema, or write Java's tables directly. Its local execution row `id` is a storage identity only and does not replace `task_id` or `operation_id` in the protocol.

Independent data ownership does not require a separate database instance. A shared instance can host separate schemas and access permissions for the services. Connector transactions cover connector-owned records only; Java state changes arrive through the internal protocol. The current implementation selects MySQL 8.0 or newer with a provisioned connector schema separate from Java. Deployment and retention settings remain implementation choices.

### Core logical records

`generation_execution` and `outbox_event` are the core logical records; the fields below describe their meaning. Their physical MySQL schema and guarded migration are implemented separately.

The current [MySQL schema](../../../src/lux_model_connector/schema.sql) implements these two records, `command_receipt`, `kafka_receipt`, `generation_result`, and `connector_schema_version`. Request archives and outcomes use `LONGTEXT` with `JSON_VALID` checks where applicable to preserve numeric values. Owned v1 schemas migrate additively to v2, adding nullable execution-claim fields and result records; the packaged [v1 snapshot](../../../src/lux_model_connector/schema_v1.sql) validates the source. Guarded steps can resume interrupted nontransactional MySQL DDL. Foreign or incompatible schemas remain rejected. Initialization validates columns, types, nullability, defaults, explicit collations, auto-increment, required primary/unique indexes, and InnoDB. SQL calls run outside the event loop. Publication uses separate 30-second database-time claims and 2-to-60-second retry delays; provider submission and preservation use 15-minute execution claims. Read-only provider polling requires no execution claim.

| `generation_execution` field | Type and purpose |
| --- | --- |
| `id` | Positive 64-bit integer; independent local row primary key. Its generation mechanism is chosen with the database. |
| `user_id`, `task_id` | UUIDs; ownership and Java Task correlation, protected by a unique constraint rather than used as the primary key. |
| `session_id`, `operation_id` | UUIDs; immutable Session correlation and submission idempotency identity. |
| `input_generation_payload` | Immutable snapshot of the received request body, represented by named `InputGenerationPayload` and stored as JSON if appropriate. Executable work requires decoding into the defined request type. Invalid input may be archived only as diagnostic evidence at the typed rejection boundary; compare that original content for conflicts, never two null decoded requests. No separate hash column is required. |
| `status` | Exactly `ACCEPTED`, `RUNNING`, `SUCCEEDED`, or `FAILED`. |
| `submission_payload` | Nullable named `ProviderSubmissionPayload`: `provider_id`, `account_ref`, `kind`, and the exact named request snapshot. Records which provider/account to call and what to send; fixed before the first submission, not evidence that it was accepted. |
| `submission_started_at` | Nullable timestamp committed before any provider submission; a marker of possible submission, not proof of provider acceptance. |
| `provider_external_id` | Optional task, request, or operation ID returned by the external generation API, used to query the original operation. An established ID cannot be replaced. Synchronous APIs may not return one. |
| `outcome` | Nullable `GenerationOutcome`; the connector's final answer, required at a terminal state and immutable thereafter. Success contains the complete ordered result-source manifest; failure contains `GenerationError`. |
| `terminal_event_id` | Optional UUID assigned with the first terminal outcome; points to its original immutable Outbox event so retries and replay find the same notification. |
| `next_event_sequence` | Positive signed 64-bit integer used to allocate the next observation sequence atomically. Events carry it as `sequence_no`; Java observes it as `provider_event_seq`. Separate from Java Task versions and Agent/Session counters. |
| `created_at`, `updated_at`, `started_at`, `finished_at` | Timestamps; `started_at` records entry into `RUNNING`, and `finished_at` exists exactly for terminal executions. |

Enforce uniqueness of both `(user_id, task_id)` and `(user_id, operation_id)`. The former retains one execution per business Task in this scope; the latter deduplicates submissions. The independent primary key does not replace these business constraints. Duplicate operations must preserve the original Task, Session, and request identity. Invalid ingress evidence is retained at the typed receipt/diagnostic boundary; it is not processed as an anonymous business dictionary. A failed request-validation record may omit submission payload and provider evidence.

The following fields support provider recovery, worker coordination, or diagnostics. Their necessity depends on the selected provider and executor; they are not caller protocol fields:

| Conditional execution field | When and why it is kept |
| --- | --- |
| `provider_receipt` | Named evidence derived from the provider API response, only if `provider_external_id` and the fixed submission are insufficient to recover. For example, a synchronous API may return output locations without a task ID. Preserve the response fields required to recover the same output; archiving the entire response is not mandatory. |
| `provider_completed_at` | Optional time at which definitive provider completion was observed; useful for diagnostics but not proof of complete result preservation. |
| `execution_lease_token`, `execution_lease_until` | Submission or result-preservation claim token and expiry; the implemented claim lasts 15 minutes and is renewed during result work. A takeover changes the token, and claimed-work writes require its current unexpired token. Receipt checkpointing for the same immutable original submission can accept a late response after lease expiry; losing a lease must not discard the only provider acceptance evidence. Read-only polling does not claim these fields. Outbox claims remain separate. |
| `next_reconcile_at` | Deferred for the initial polling version. A future per-execution polling interval or backoff may use this timestamp; fixed periodic scanning does not require it. It never schedules new generation. |
| `last_reconcile_at` | Optional diagnostic timestamp of the last provider status check; omitted from the initial required field set. |
| `last_observation_error` | Optional typed diagnostic of the last polling/query error; omitted from the initial required field set. An observation error alone is not a definitive generation failure. |

An asynchronous provider with durable task lookup may need only `provider_external_id` after submission. A provider that cannot recover its synchronous response requires the relevant response checkpoint before result preparation continues. Interpreted response fields always have explicit types; an opaque original response may be archived only for diagnosis.

### Worker ownership and observation scheduling

The initial polling mechanism gives no worker permanent responsibility for an execution. Every worker runs its own periodic loop against the durable execution records. A worker does not wait for another poller's lease to expire, and a restart does not require restoring an in-memory list of owned polling tasks.

Each loop:

1. Scans bounded pages of `RUNNING` executions that have a `provider_external_id` and whose provider supports safe repeated, read-only status queries. A scan must eventually visit every eligible execution rather than repeatedly selecting only the first page.
2. Queries the original provider/account/external ID with bounded local concurrency and request timeouts. A worker permits only one active scan cycle at a time; different workers may query the same operation concurrently.
3. Converts each response into a typed provider observation and applies it through the common database observation path. An individual query failure does not abort unrelated work in the scan.
4. Waits the configured fixed polling interval before its next cycle. Startup and later cycles rediscover work from durable records; no per-execution scheduling timestamp is required.

The scan is one recovery path, not the whole execution recovery mechanism. `ACCEPTED` executions still require submission processing. A `RUNNING` execution without a provider external ID may require uncertain-submission lookup or recovery from a synchronous response checkpoint. Interrupted result preservation must recover the original output even when it is not eligible for status polling. These cases retain their dedicated recovery paths; an empty external ID never authorizes a fresh generation.

Applying observations must reread the current execution and enforce legal transitions with atomic conditional updates, transactions, and uniqueness constraints. A late running observation cannot overwrite a terminal outcome. Timeouts, query errors, or an inconclusive not-found response do not establish generation failure. Duplicate observations do not allocate repeated progress events merely because another polling cycle occurred. An explicit Java status-query command remains a separately deduplicated request and may receive a new response event.

Provider success becomes connector success only after the complete original output has been preserved. Concurrent workers must preserve the same output idempotently or use a separate result-preservation claim; they cannot overwrite an accepted manifest with competing source sets. Once the outcome is ready, one conditional terminal transition commits the outcome, original terminal event ID, sequence allocation, and Outbox together. A competing worker rereads that committed outcome, avoids creating a second original terminal event, and retains conflicting terminal evidence for diagnosis rather than selecting an outcome solely by response arrival order.

For example, worker A's query can return an old running observation after worker B has preserved the results and committed success. The observation application path rejects regression without requiring polling ownership. If A crashes, B continues its ordinary scan of durable records; there is no polling lease handoff.

Independent scans intentionally permit duplicate provider queries. Their request volume grows with worker count, so the initial version uses bounded local concurrency and a fixed interval. This choice favors a simple recoverable loop; coordinated polling or per-execution backoff can be added if actual query load requires it. `next_reconcile_at`, `last_reconcile_at`, and `last_observation_error` are not required by this initial polling mechanism.

Submission, result preservation, and Outbox publication are separate from read-only polling. Where they use database claims, lease expiry grants local ownership only and stale claim writes are rejected. It does not stop an in-flight provider call or authorize another paid generation. Independent polling does not remove those operations' idempotency, ordering, or stale-acknowledgement protections.

| `outbox_event` field | Type and purpose |
| --- | --- |
| `event_id` | UUID primary key; stable transport-event identity. |
| `user_id`, `task_id`, `sequence_no` | Execution correlation and positive observation sequence; unique together. |
| `topic`, `message_key`, `envelope` | Destination, partition key, and complete named envelope snapshot, including original event type, time, identities, and payload. These do not change during retry or replay. |
| `created_at`, `published_at` | Creation time and optional broker-publication acknowledgement time. `published_at` does not prove Java application or Asset import. |
| `attempt_count`, `next_attempt_at`, `last_error` | Nonnegative delivery attempt count, retry schedule, and optional publication diagnostic. |
| `lease_token`, `lease_until` | Publication claim token and expiry; stale acknowledgement handling cannot complete a newer publication attempt. |

The execution outcome, terminal event identity, sequence allocation, and terminal Outbox insertion commit together. A terminal execution has exactly one original terminal-outcome event. Status-query responses are separate observations and must not create additional terminal transitions. The publication worker preserves per-Task ordering; a pending earlier event cannot be overtaken by a newly allocated event.

### Receipt records

The two core records do not automatically implement every ingress contract. A submission's execution row can provide durable acceptance and operation-level deduplication. Preserving all command event identities, diagnosing changed content under the same event ID, or accepting a webhook before its asynchronous application requires a durable receipt mechanism.

Add an Inbox/receipt record when that mechanism cannot be represented safely in the execution record. It retains the input identity, named command or observation, payload identity, receipt/application progress, and rejection diagnostic where relevant. Ingress progress is independent of the four execution states. Malformed input may be archived as opaque evidence inside a named rejection record; it must not become a known business request with undefined fields.

### Transactions and recovery

| Boundary or failure window | Required behavior |
| --- | --- |
| Accept a submission | Save acceptance and the execution together before acknowledging Kafka consumption; save any separately required receipt in that transaction. |
| Reject invalid known request shape after durable admission | The asynchronous worker commits the failed execution, outcome, sequence, and failure Outbox event together; recovery uses the already acknowledged durable execution. |
| Crash after entry into `RUNNING`, before submission payload | Recover preparation; a first submission is allowed only after persisting its payload and marker under a valid claim. |
| Crash after submission payload, before confirmed receipt | Reconcile the original provider/account/request; do not infer non-submission from an empty provider external ID or an expired lease. |
| Persist provider completion but interrupt result preparation | Resume preservation of the same output while remaining `RUNNING`. |
| Interrupt an independent polling cycle | A later cycle on any worker rediscovers eligible durable executions; no polling lease takeover is required. |
| Apply concurrent or late polling observations | Conditional updates preserve terminal immutability; duplicate observations create no second original terminal event. |
| Reach `SUCCEEDED` or `FAILED` | Commit terminal state, immutable outcome, original event identity, sequence, and Outbox together. |
| Lose broker publication acknowledgement | Retry the same immutable event; update delivery information without changing execution outcome. |
| Replay an already published event | Preserve its ID, sequence, time, and payload. Reopening its delivery schedule invalidates an old publication claim, so a late acknowledgement cannot clear the requested replay. |
| Answer a new status query | Allocate a new response event ID and sequence for the current observation, subject to query deduplication; do not overwrite the original terminal event. |

Database transactions do not include Kafka publication, provider calls, or result transfers. Persisted submission evidence supports recovery but does not make an external call atomic with the database. An expired lease grants local recovery ownership; it does not cancel an already issued provider request or authorize another generation.

## Provider observations

Provider adapters normalize synchronous responses, asynchronous status queries, and webhooks into the same typed observations. Only mechanisms supported by the selected provider are implemented.

A webhook adapter verifies the provider's authentication mechanism and task correlation, persists the observation before acknowledging it, and tolerates duplicates and late delivery. Processing a webhook after receipt must be recoverable without requiring the provider to send it again.

When provider status queries are available, reconciliation can recover a missing webhook or an interrupted polling cycle. Reconciliation preserves the original operation and accepted provider task ID. The connector must not replace that ID with a new paid execution to obtain a result.

The initial query implementation follows the independent worker loops above. Provider onboarding must establish that repeating the query is safe and does not submit or consume a one-time operation. Synchronous responses and webhooks use the same observation-application rules, with their own durable receipt requirements.

## Terminal outcome delivery

The first terminal transition saves the immutable outcome and its Outbox event in the same local database transaction. The event is published outside that transaction. A crash between commit and publication leaves an event available for recovery; uncertain broker acknowledgement causes publication of the same event again.

Execution state and delivery state are separate. A completed execution can have a pending publication, a broker-confirmed publication, or a publication requiring operational attention. Publication failure does not turn successful generation into failed generation, and does not cause another provider invocation.

The connector retains a durable outcome for status queries and replay. Replaying the original terminal event preserves its original identity and content. A separate status response reports the same outcome and result manifest; it does not create a new execution or a new business completion.

Java deduplicates incoming events and applies the relevant Task transition durably. Its first business terminal transition creates the unique `SYSTEM/TASK_EVENT` Session input with content containing only `task_id`, together with its original `INPUT_ACCEPTED` command and SSE invalidation. Deduplication is by Task identity, not Task version or connector event sequence. Progress and duplicate terminal observations cannot create another terminal input. Python receives this through `lux.agent.commands.v2`; the connector does not publish Agent commands or update historical ToolResults. Agent input ordering, stop/wake decisions, and checkpoint incorporation remain Python responsibilities.

## Result preservation and Asset import

Connector execution success requires a complete result-source manifest whose content can be retained or recovered for Java import. Under the existing import boundary, sources are complete HTTP(S) resources returning 200 without redirects. An expiring provider URL is insufficient unless the adapter can preserve or recover the same output independently.

The accepted manifest has stable ordering and does not change after Java starts importing it. Recovery restores access to the same content; it does not substitute newly generated output. Multiple outputs are successful only when the complete required manifest is available.

If renewing a provider URL would change the accepted manifest, use a durable source URL or a stable connector-served URL before reporting success. Updating an expiring URL after Java has accepted the manifest is not compatible with this import boundary.

BytePlus output URLs expire after 24 hours. The implementation preserves original bytes in a provisioned private S3 or S3-compatible bucket, with immutable object keys derived from owner, Task, and source index. `generation_result` records the stable index, storage identity, object metadata/hash/length, and a random 256-bit read capability. An upload whose SQL checkpoint was lost is recovered through matching existing object metadata; it does not require another provider download or generation. All required original outputs, including requested video last-frame output or image layers, must be retained before success.

The success manifest uses `${LUX_CONNECTOR_RESULT_BASE_URL}/internal/generation-results/{token}`. This connector gateway streams private retained bytes as HTTP 200 with stable content metadata and does not redirect to expiring presigned URLs. Treat each capability URL as a read credential within the trusted Java/connector boundary; API keys and S3 secrets never appear in it. Configure service routing and retention to keep the same URL available across restart and Outbox replay. Provider response reads are limited to 128 MiB and each retained media file to 100 MiB. Transfer/storage uncertainty, including provider URL 404/410, keeps the execution `RUNNING`: a negative object lookup does not prove an earlier uncertain upload cannot arrive later. Subsequent scans check the immutable object first and recover it if available. An expired URL without confirmed retained output requires operational reconciliation, never another generation.

| Observation | Java business behavior |
| --- | --- |
| Connector reports generation success with result sources | Task enters `IMPORTING`. |
| Java verifies and durably stores all required bytes and Asset metadata | Task enters `SUCCEEDED` and emits its unique Session terminal input. |
| Connector reports definitive generation failure | Java records a generation-stage Task failure. |
| Java records definitive import failure | Task enters `FAILED`; any already valid partial Assets remain available. |

Java may recover an interrupted, unconfirmed import and reuse verified Assets at their original zero-based `source_index`. Their stable Task/source/Asset identities preserve the accepted manifest order across redelivery. `get_task` returns these Asset references rather than provider download URLs. A recorded import failure ends the Task; it is not an instruction to reopen import or repeat provider generation.

For a valid callback/source, confirmed GET/connection/read failure, including HTTP 5xx or a per-source timeout while the whole-import deadline remains valid, and content-validation failure are Java IMPORT failures: persist FAILED, its notification, and Inbox APPLIED, then acknowledge consumption. Exhaustion of the whole-import transfer budget, database failure, process/consumer-ownership interruption, or ambiguous object-publication outcome instead requires unacknowledged technical recovery/reconciliation of the same event and Asset identities. Conflicting or malformed event identity/ownership is rejected with diagnostics without changing the original Task. Immutable object publication and Task state/version conditions protect against duplicate or stale consumers; this classification introduces no business Task retry.

Output cleanup must be governed by import completion confirmation or an explicitly agreed retention and recovery policy. Kafka publication is not an output-release signal. Until that policy is defined, publication success must not trigger automatic deletion of retained output.

## Legacy compatibility and remaining implementation

The Java [v1 contract](../../../../lux-java-backend/docs/api-contract-v1.md) is historical. The active integration baseline is [v2](../../../../lux-java-backend/docs/api-contract-v2.md). Java has an explicitly separated v1 generation receive adapter with these mappings:

| Legacy callback | Current Java adaptation |
| --- | --- |
| `TASK_SUBMITTED`, `TASK_RUNNING` | `RUNNING`; old provider acknowledgement does not become connector `ACCEPTED`. |
| `TASK_RESULT` | `SUCCEEDED` with legacy top-level `sources` converted into the named outcome. |
| `TASK_FAILED` | `FAILED` with legacy `error_message`, generation stage `PROVIDER`, and compatibility error code. |
| `TASK_STATUS` | Old `SUBMITTED` becomes `RUNNING`; terminal states use their corresponding outcomes; `UNKNOWN` remains an unconfirmed observation, not a fifth connector execution state. |

The current adapter does not copy `provider_task_id` into Java Task fields. New connector events use the four-state v2 envelope and keep `provider_external_id` and `provider_receipt` connector-owned. Keep v2 objects on v2 topics and old Run-bound commands isolated. The historical `TASK_UPDATED` ToolMessage replacement flow is retired.

Java and Python use their implemented v2 storage baselines and consumer contracts. The connector implements durable command reception, status-query observations, saved-byte Outbox publication, optional BytePlus execution, independent polling, and S3 preservation on that generation path. Enable its v2 consumer and provider execution through explicit runtime configuration, and account for old messages/data rather than silently rewriting their meaning. Shared v2 fixtures validate Java/Agent contract examples; local connector checks do not establish live cross-service generation or paid provider behavior.

MySQL, PyMySQL with RSA support, aiokafka, HTTPX, and boto3 are selected. BytePlus request schemas and the S3 preservation boundary are implemented; API keys, model/account activation, live cloud configuration, Java Asset-ID input resolution, webhook reception, and output retention duration remain deployment or subsequent implementation work. Live generation stays disabled until explicitly configured.

## Verification requirements

| Scenario | Observable guarantee |
| --- | --- |
| Duplicate submission, including changed content under the same identity | One accepted execution; conflicts are rejected and retained. |
| V2 submit/status-query command decoding and topic/version checks | Flat owner/Session/Task envelopes match Java; no Run/tool/provider ID is required in the command. |
| Java submission receipt precedes connector admission | Stable Task receipt does not falsely report connector acceptance or provider execution. |
| Generation continues after the originating Run ends | Same operation completes and Java emits an independent Session terminal input. |
| Crash before or after durable acceptance | Unacknowledged input is redelivered, or persisted input is recovered without loss. |
| Competing workers or an expired lease | One submission owner; stale writes are rejected and takeover does not blindly resubmit. |
| Different workers poll the same provider operation | Repeated read-only queries are allowed; no polling lease is required and observations preserve the original operation. |
| An old running response arrives after committed success | No terminal regression and no duplicate terminal notification. |
| Two workers observe completion concurrently | Same original output is preserved safely; one terminal commit and one original terminal Outbox event. |
| Polling worker restarts or a scan/query is interrupted | Durable eligible work is rediscovered; later pages and unrelated tasks are not starved. |
| Query times out or returns an inconclusive missing task | Execution remains nonterminal and later scans can retry observation without resubmitting generation. |
| Crash between entry into `RUNNING` and submission payload | Recover preparation without falsely assuming an external submission, and persist the payload before a first call. |
| Provider accepts but response or local receipt write is interrupted | Recovery reconciles the original operation without creating a second paid task. |
| Submission remains uncertain or provider success precedes complete result preservation | Execution remains `RUNNING`; neither condition manufactures a terminal outcome. |
| Duplicate or late provider webhook | One valid observation effect; terminal state remains immutable. |
| Crash during terminal commit and publication | Outcome and Outbox record remain consistent; replay retains the same event. |
| Broker acknowledgement is lost or Java is temporarily unavailable | Delivery resumes without changing the outcome or invoking the provider again. |
| Replay overlaps a late publication acknowledgement | The original event stays unchanged, and the old claim cannot clear the newer replay request. |
| Result access expires or import is interrupted | Restore the same result where supported; reuse verified Assets without regenerating. |
| Provider success followed by Java import failure | Connector outcome remains successful; business Task reports import failure with no retry linkage. |
| Duplicate terminal observations after Java import/failure | One Task terminal input and no rewriting of the original Python ToolResult. |
| Agent reads a Task after its terminal notice | A new immutable query snapshot contains ordered Asset IDs/source indexes or GENERATION/IMPORT failure. |
| Invalid outcome/source/error limits or oversized envelope | Reject incompatible delivery data without truncation or another provider invocation. |
| Source timeout versus whole-import transfer-budget exhaustion | Confirmed source failure commits IMPORT failure; technical budget exhaustion remains unacknowledged for the same import's recovery. |
| Legacy callbacks alongside v2 observations | Explicit topic/version adaptation preserves four-state semantics and original event identities. |
| Invalid known request shape or unsupported image streaming | Durable `VALIDATION` failure without a provider call; supplied fields are not silently discarded. |
| Arbitrary upstream Model/Endpoint ID or model-specific option combination | Exact Model ID and supplied options reach BytePlus; remote rejection is retained without model substitution or automatic retry. |
| Original S3 upload succeeds but SQL checkpoint is lost | Existing immutable object metadata recovers the same indexed output without a new provider GET or generation. |
| Execution flag is absent or false despite configured credentials | Ingestion remains available but no provider generation starts. |

Use deterministic provider doubles for state, persistence, and recovery tests. Real Kafka and Java integration must separately verify serialization, acknowledgement ordering, duplicate handling, and result import. Paid live-provider tests require their own approved target, scope, budget, and stop policy.
