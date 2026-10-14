"""Generation v2 wire and immutable storage-snapshot contract."""

import importlib
import json
from types import ModuleType

import pytest
from pydantic import JsonValue, ValidationError

EVENT_ID = "aaaaaaaa-0000-0000-0000-000000000001"
USER_ID = "bbbbbbbb-0000-0000-0000-000000000001"
SESSION_ID = "cccccccc-0000-0000-0000-000000000001"
TASK_ID = "dddddddd-0000-0000-0000-000000000001"
OPERATION_ID = "eeeeeeee-0000-0000-0000-000000000001"
TIMESTAMP = "2026-10-12T12:34:56.120000Z"
BUDGET = 31 * 1024 * 1024


@pytest.fixture
def protocol() -> ModuleType:
    return importlib.import_module("lux_model_connector.protocol")


def submit(**changes: JsonValue) -> dict[str, JsonValue]:
    return {
        "schema_version": 2,
        "event_id": EVENT_ID,
        "event_type": "SUBMIT",
        "occurred_at": TIMESTAMP,
        "user_id": USER_ID,
        "session_id": SESSION_ID,
        "task_id": TASK_ID,
        "operation_id": OPERATION_ID,
        "kind": "IMAGE",
        "request": {"model_id": "future-model", "prompt": " 雪\n", "count": 1},
        **changes,
    }


def query(**changes: JsonValue) -> dict[str, JsonValue]:
    result = submit(event_type="TASK_STATUS_QUERY", last_observed_sequence_no=0)
    for field in ("operation_id", "kind", "request"):
        del result[field]
    return {**result, **changes}


def event(**changes: JsonValue) -> dict[str, JsonValue]:
    result = query()
    del result["last_observed_sequence_no"]
    return {
        **result,
        "event_type": "GENERATION_EVENT",
        "sequence_no": 1,
        "connector_status": "ACCEPTED",
        "outcome": None,
        **changes,
    }


def source(**changes: JsonValue) -> dict[str, JsonValue]:
    return {
        "url": "https://media.example.com/result.png?token=opaque",
        "filename": "雪.png",
        "media_type": "image/png",
        **changes,
    }


def outcome(status: str) -> dict[str, JsonValue]:
    if status == "SUCCEEDED":
        return {"status": status, "sources": [source()]}
    return {
        "status": status,
        "error": {"code": "PROVIDER_REJECTED", "stage": "PROVIDER", "message": "拒绝"},
    }


def test_submit_preserves_archived_json_and_named_identity(
    protocol: ModuleType,
) -> None:
    request: dict[str, JsonValue] = {
        "extension": [None, True, False, 0, 1.25, " 雪\n", {"explicit_null": None}],
        "asset_ids": ["opaque-existing-reference"],
    }
    command = protocol.decode_command(json.dumps(submit(request=request)).encode())
    assert isinstance(command, protocol.GenerationSubmit)
    assert isinstance(command.request, protocol.ArchivedGenerationRequest)
    assert command.request.model_dump() == request
    assert command.occurred_at == TIMESTAMP
    snapshot = protocol.InputGenerationPayload(
        identity=protocol.GenerationIdentity(
            user_id=command.user_id,
            session_id=command.session_id,
            task_id=command.task_id,
            operation_id=command.operation_id,
        ),
        kind=command.kind,
        request=command.request,
    )
    assert snapshot.model_dump(mode="json") == {
        "identity": {
            "user_id": USER_ID,
            "session_id": SESSION_ID,
            "task_id": TASK_ID,
            "operation_id": OPERATION_ID,
        },
        "kind": "IMAGE",
        "request": request,
    }
    assert "missing" not in command.request.model_dump()
    assert command.request.model_dump()["extension"][-1] == {"explicit_null": None}
    with pytest.raises(ValidationError):
        command.kind = "VIDEO"


def test_query_zero_and_maximum_watermark(protocol: ModuleType) -> None:
    for watermark in (0, 2**63 - 1):
        command = protocol.decode_command(
            json.dumps(query(last_observed_sequence_no=watermark))
        )
        assert isinstance(command, protocol.TaskStatusQuery)
        assert command.last_observed_sequence_no == watermark


@pytest.mark.parametrize("field", ["schema_version", "last_observed_sequence_no"])
@pytest.mark.parametrize("value", [True, False, 2.0, "2", -1, 2**63])
def test_rejects_non_integer_or_out_of_range_counters(
    protocol: ModuleType, field: str, value: JsonValue
) -> None:
    with pytest.raises(ValueError):
        protocol.decode_command(json.dumps(query(**{field: value})))


@pytest.mark.parametrize(
    "field", ["event_id", "user_id", "session_id", "task_id", "operation_id"]
)
@pytest.mark.parametrize(
    "value", [EVENT_ID.upper(), EVENT_ID.replace("-", ""), "invalid", 1]
)
def test_ids_require_canonical_lowercase_uuid(
    protocol: ModuleType, field: str, value: JsonValue
) -> None:
    with pytest.raises(ValueError):
        protocol.decode_command(json.dumps(submit(**{field: value})))


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-12T12:34:56Z",
        "2026-10-12T12:34:56.123456Z",
        "2026-10-12T12:34:56.120000+00:00",
    ],
)
def test_preserves_valid_utc_timestamp_text(
    protocol: ModuleType, timestamp: str
) -> None:
    command = protocol.decode_command(json.dumps(submit(occurred_at=timestamp)))
    assert command.occurred_at == timestamp


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-12T12:34:56.1234567Z",
        "2026-10-12T12:34:56+01:00",
        "2026-10-12T12:34:56",
        "2026-02-30T12:34:56Z",
        "not-a-time",
        1,
    ],
)
def test_rejects_invalid_non_utc_or_overprecision_time(
    protocol: ModuleType, timestamp: JsonValue
) -> None:
    with pytest.raises(ValueError):
        protocol.decode_command(json.dumps(submit(occurred_at=timestamp)))


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 1},
        {"event_type": "INPUT_ACCEPTED"},
        {"run_id": EVENT_ID},
        {"kind": "AUDIO"},
        {"request": None},
        {"request": []},
        {"request": "opaque"},
        {"payload": {}},
        {"tool_call_id": "call-1"},
    ],
)
def test_rejects_unsupported_or_nested_commands(
    protocol: ModuleType, change: dict
) -> None:
    with pytest.raises(ValueError):
        protocol.decode_command(json.dumps(submit(**change)))


@pytest.mark.parametrize("field", ["request", "kind", "schema_version", "event_type"])
def test_submit_requires_declared_members(protocol: ModuleType, field: str) -> None:
    body = submit()
    del body[field]
    with pytest.raises(ValueError):
        protocol.decode_command(json.dumps(body))


@pytest.mark.parametrize(
    "raw",
    [
        '{"schema_version":2,"schema_version":2}',
        json.dumps(submit()).replace('"count": 1', '"count":1,"count":1'),
        json.dumps(submit()).replace('"count": 1', '"count":NaN'),
        json.dumps(submit()).replace('"count": 1', '"count":Infinity'),
        json.dumps(submit()).replace('"count": 1', '"count":1e999'),
        b"\xff",
        "[]",
        "null",
        "{",
        json.dumps(submit()).replace("future-model", "\\ud800"),
    ],
)
def test_rejects_malformed_nonfinite_or_ambiguous_json(
    protocol: ModuleType, raw: bytes | str
) -> None:
    with pytest.raises(ValueError):
        protocol.decode_command(raw)


def test_canonical_json_preserves_values_and_unicode(protocol: ModuleType) -> None:
    value: JsonValue = {"z": [False, None, 1.0, "雪\n"], "a": {"y": 2, "b": " "}}
    encoded = protocol.canonical_json(value)
    assert encoded == '{"a":{"b":" ","y":2},"z":[false,null,1.0,"雪\\n"]}'
    assert json.loads(encoded) == value
    for invalid in (float("nan"), float("inf"), {"nested": float("-inf")}):
        with pytest.raises(ValueError):
            protocol.canonical_json(invalid)


@pytest.mark.parametrize("status", ["ACCEPTED", "RUNNING", "SUCCEEDED", "FAILED"])
def test_encodes_all_observations_with_matching_outcome(
    protocol: ModuleType, status: str
) -> None:
    body = event(
        connector_status=status,
        outcome=outcome(status) if status in ("SUCCEEDED", "FAILED") else None,
    )
    del body["event_type"]
    value = protocol.GenerationEvent.model_validate(body)
    encoded = protocol.encode_event(value)
    assert json.loads(encoded) == {**body, "event_type": "GENERATION_EVENT"}
    if status == "SUCCEEDED":
        assert isinstance(value.outcome.sources, tuple)
    assert protocol.GenerationEvent.model_validate_json(encoded) == value


@pytest.mark.parametrize(
    "status,detail",
    [
        ("ACCEPTED", outcome("SUCCEEDED")),
        ("RUNNING", outcome("FAILED")),
        ("SUCCEEDED", None),
        ("FAILED", None),
        ("SUCCEEDED", outcome("FAILED")),
        ("FAILED", outcome("SUCCEEDED")),
        ("SUBMITTED", None),
        ("UNKNOWN", None),
    ],
)
def test_status_and_outcome_must_agree(
    protocol: ModuleType, status: str, detail: JsonValue
) -> None:
    with pytest.raises(ValueError):
        protocol.GenerationEvent.model_validate(
            event(connector_status=status, outcome=detail)
        )


@pytest.mark.parametrize("sequence", [0, -1, True, 1.0, "1", 2**63])
def test_event_sequence_is_positive_signed64(
    protocol: ModuleType, sequence: JsonValue
) -> None:
    with pytest.raises(ValueError):
        protocol.GenerationEvent.model_validate(event(sequence_no=sequence))


@pytest.mark.parametrize("count", [0, 65])
def test_success_requires_complete_bounded_manifest(
    protocol: ModuleType, count: int
) -> None:
    with pytest.raises(ValueError):
        protocol.GenerationSuccessOutcome(
            status="SUCCEEDED", sources=[source()] * count
        )


@pytest.mark.parametrize(
    "change",
    [
        {"url": "ftp://media.example.com/a"},
        {"url": "/relative"},
        {"url": "http://user:secret@example.com/a"},
        {"url": "https:///a"},
        {"url": "https://example.com/a b"},
        {"url": "https://example.com/%broken"},
        {"filename": " "},
        {"filename": "a/b"},
        {"filename": "a\\b"},
        {"filename": "a\x00b"},
        {"filename": "a\x7fb"},
        {"filename": "x" * 256},
        {"filename": "😀" * 128},
        {"filename": 5},
        {"media_type": ""},
        {"media_type": "image/*"},
        {"media_type": "image/*+xml"},
        {"media_type": "*/png"},
        {"media_type": "not-a-media-type"},
        {"media_type": "image/"},
        {"media_type": "image/" + "x" * 123},
    ],
)
def test_result_sources_match_java_import_contract(
    protocol: ModuleType, change: dict
) -> None:
    with pytest.raises(ValueError):
        protocol.ResultSource.model_validate(source(**change))


def test_valid_source_and_error_boundary_values(protocol: ModuleType) -> None:
    protocol.GenerationSuccessOutcome(status="SUCCEEDED", sources=[source()] * 64)
    protocol.ResultSource.model_validate(
        source(filename="x" * 255, media_type="image/" + "x" * 122)
    )
    protocol.ResultSource.model_validate(
        source(url="HTTPS://example.com/a", media_type='image/png; name="a;b"')
    )
    error = protocol.GenerationError(
        code="x" * 64, stage="PROVIDER", message="雪" * 21845
    )
    assert len(error.message.encode()) == 65535


@pytest.mark.parametrize(
    "change",
    [
        {"code": " "},
        {"code": "x" * 65},
        {"code": "😀" * 33},
        {"message": "\n"},
        {"message": "雪" * 21846},
        {"message": 1},
        {"stage": "IMPORT"},
        {"provider_diagnostic": {"provider_id": " "}},
        {"provider_diagnostic": {"provider_id": "p", "secret": "x"}},
    ],
)
def test_errors_validate_classification_and_utf8_capacity(
    protocol: ModuleType, change: dict
) -> None:
    with pytest.raises(ValueError):
        protocol.GenerationError.model_validate(
            {"code": "X", "stage": "PROVIDER", "message": "Error", **change}
        )


def test_named_provider_diagnostic_round_trips(protocol: ModuleType) -> None:
    error = protocol.GenerationError(
        code="NATIVE_ERROR",
        stage="PROVIDER",
        message="Error",
        provider_diagnostic=protocol.ProviderErrorDiagnostic(
            provider_id="provider",
            provider_code="long-native-code",
            provider_message=None,
        ),
    )
    value = protocol.GenerationFailureOutcome(status="FAILED", error=error)
    assert value.model_dump(mode="json")["error"]["provider_diagnostic"] == {
        "provider_id": "provider",
        "provider_code": "long-native-code",
        "provider_message": None,
    }


def test_complete_utf8_command_budget(protocol: ModuleType) -> None:
    base = json.dumps(
        submit(request={"text": ""}), ensure_ascii=False, separators=(",", ":")
    )
    remaining = BUDGET - len(base.encode())
    raw = base.replace(
        '"text":""', '"text":"' + "雪" * (remaining // 3) + "x" * (remaining % 3) + '"'
    )
    assert len(raw.encode()) == BUDGET
    protocol.decode_command(raw.encode())
    with pytest.raises(ValueError):
        protocol.decode_command(raw + " ")


def test_complete_utf8_event_budget(protocol: ModuleType) -> None:
    value = protocol.GenerationEvent.model_validate(
        event(connector_status="SUCCEEDED", outcome=outcome("SUCCEEDED"))
    )
    initial = protocol.encode_event(value)
    initial_url = value.outcome.sources[0].url
    replacement_url = initial_url + "x" * (BUDGET - len(initial.encode()))
    bounded = protocol.GenerationEvent.model_validate(
        event(
            connector_status="SUCCEEDED",
            outcome={"status": "SUCCEEDED", "sources": [source(url=replacement_url)]},
        )
    )
    assert len(protocol.encode_event(bounded).encode()) == BUDGET
    oversized = bounded.model_copy(
        update={
            "outcome": protocol.GenerationSuccessOutcome(
                status="SUCCEEDED",
                sources=[source(url=replacement_url + "x")],
            )
        }
    )
    with pytest.raises(ValueError):
        protocol.encode_event(oversized)


@pytest.mark.parametrize(
    "url",
    [
        "https://a..com/x",
        "https://a.-b.com/x",
        "https://999.999.999.999/x",
        "https://example.123/x",
        "https://example.com:2147483648/x",
        "https://example.com/a[b]",
    ],
)
def test_result_url_rejects_missing_java_uri_host(
    protocol: ModuleType, url: str
) -> None:
    with pytest.raises(ValueError):
        protocol.ResultSource.model_validate(source(url=url))


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com:65536/x",
        "https://[::1]:65536/x",
        "https://127.0.0.1/x",
        "https://123/x",
        "https://example.com/x?q=[a]",
    ],
)
def test_result_url_accepts_java_uri_hosts(protocol: ModuleType, url: str) -> None:
    assert protocol.ResultSource.model_validate(source(url=url)).url == url


@pytest.mark.parametrize(
    "media_type",
    [
        "image/png;charset=unknown-xyz",
        "image/png;q=2",
        "image/png;q=NaN",
        "image/png;name=",
        'image/png;name="a',
    ],
)
def test_media_type_rejects_invalid_java_parameters(
    protocol: ModuleType, media_type: str
) -> None:
    with pytest.raises(ValueError):
        protocol.ResultSource.model_validate(source(media_type=media_type))


@pytest.mark.parametrize(
    "media_type",
    [
        " image/png ",
        "image/png;foo",
        "image/png;",
        "image/p*ng",
        "image/png;charset=UTF-8",
        'image/png;name="a;b"',
        "image/png;q=.2",
    ],
)
def test_media_type_preserves_java_parameter_semantics(
    protocol: ModuleType, media_type: str
) -> None:
    assert (
        protocol.ResultSource.model_validate(source(media_type=media_type)).media_type
        == media_type
    )
