"""Typed generation v2 commands, observations, and archival JSON boundaries."""

import codecs
import json
import math
import re
from datetime import datetime
from ipaddress import IPv6Address
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

MAX_ENVELOPE_BYTES = 31 * 1024 * 1024
MAX_SEQUENCE_NO = 2**63 - 1


def _uuid(value: str) -> str:
    if str(UUID(value)) != value:
        raise ValueError("IDs must be canonical lowercase UUIDs")
    return value


def _timestamp(value: str) -> str:
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value
    ):
        raise ValueError("Timestamps must be UTC with at most microsecond precision")
    datetime.fromisoformat(value)
    return value


def _version(value: JsonValue) -> int:
    if type(value) is not int or value != 2:
        raise ValueError("schema_version must be the integer 2")
    return value


def _utf16_length(value: str) -> int:
    # Java bounds use String.length(), which counts UTF-16 units, not code points.
    return len(value.encode("utf-16-le")) // 2


def _nonblank(value: str) -> str:
    # Java Character.isWhitespace excludes the three nonbreaking spaces.
    if not value or all(
        character.isspace() and character not in "\u0085\u00a0\u2007\u202f"
        for character in value
    ):
        raise ValueError("Value must be nonblank")
    value.encode("utf-8")
    return value


CanonicalUuid = Annotated[StrictStr, AfterValidator(_uuid)]
UtcTimestamp = Annotated[StrictStr, AfterValidator(_timestamp)]
SchemaVersion = Annotated[Literal[2], BeforeValidator(_version)]
PositiveSequence = Annotated[StrictInt, Field(ge=1, le=MAX_SEQUENCE_NO)]
NonnegativeSequence = Annotated[StrictInt, Field(ge=0, le=MAX_SEQUENCE_NO)]
GenerationKind = Literal["IMAGE", "VIDEO"]
ConnectorExecutionStatus = Literal["ACCEPTED", "RUNNING", "SUCCEEDED", "FAILED"]
NonblankString = Annotated[StrictStr, AfterValidator(_nonblank)]


class FrozenModel(BaseModel):
    """Known business structures reject undeclared fields and field reassignment."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ArchivedGenerationRequest(RootModel[dict[str, JsonValue]]):
    """Opaque request object stored unchanged during ingestion-only execution.

    This phase owns no model/provider schema and never interprets these values.
    Provider execution must first decode the archive into its named request type.
    """

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_archive(cls, value: JsonValue) -> JsonValue:
        if not isinstance(value, dict):
            raise ValueError("Generation request must be a JSON object")
        canonical_json(value)
        return value


class GenerationIdentity(FrozenModel):
    user_id: CanonicalUuid
    session_id: CanonicalUuid
    task_id: CanonicalUuid
    operation_id: CanonicalUuid


class InputGenerationPayload(FrozenModel):
    """Persisted submission identity and archive; never a nested wire envelope."""

    identity: GenerationIdentity
    kind: GenerationKind
    request: ArchivedGenerationRequest


class CommandHeader(FrozenModel):
    schema_version: SchemaVersion
    event_id: CanonicalUuid
    occurred_at: UtcTimestamp
    user_id: CanonicalUuid
    session_id: CanonicalUuid
    task_id: CanonicalUuid


class GenerationSubmit(CommandHeader):
    event_type: Literal["SUBMIT"]
    operation_id: CanonicalUuid
    kind: GenerationKind
    request: ArchivedGenerationRequest


class TaskStatusQuery(CommandHeader):
    event_type: Literal["TASK_STATUS_QUERY"]
    last_observed_sequence_no: NonnegativeSequence


class ProviderErrorDiagnostic(FrozenModel):
    provider_id: NonblankString
    provider_code: StrictStr | None = None
    provider_message: StrictStr | None = None


class GenerationError(FrozenModel):
    code: NonblankString
    stage: Literal["VALIDATION", "PROVIDER", "RESULT_PRESERVATION"]
    message: NonblankString
    provider_diagnostic: ProviderErrorDiagnostic | None = None

    @field_validator("code")
    @classmethod
    def validate_code(cls, value: str) -> str:
        if _utf16_length(value) > 64:
            raise ValueError("Generation error code exceeds 64 characters")
        return value

    @field_validator("message")
    @classmethod
    def validate_message(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 65535:
            raise ValueError("Generation error message exceeds 65535 UTF-8 bytes")
        return value


_TOKEN = r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+"
_JAVA_TRIM = "".join(chr(code) for code in range(33))


class ResultSource(FrozenModel):
    url: StrictStr
    filename: NonblankString
    media_type: NonblankString

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        # urlsplit is permissive; reject characters Java URI rejects before parsing.
        if re.search(r'[\x00-\x20\x7f<>"{}|\\^`]', value) or re.search(
            r"%(?![0-9a-fA-F]{2})", value
        ):
            raise ValueError("Result URL contains invalid URI characters")
        parsed = urlsplit(value)
        host = parsed.hostname
        if (
            parsed.scheme.lower() not in ("http", "https")
            or not host
            or "@" in parsed.netloc
        ):
            raise ValueError("Result URL requires HTTP(S), host, and no userinfo")
        if "[" in parsed.path or "]" in parsed.path:
            raise ValueError("Result URL path contains invalid URI brackets")
        if ":" in host:
            IPv6Address(host)
            authority = re.fullmatch(r"\[[^]]+\](?::([0-9]*))?", parsed.netloc)
        else:
            authority = re.fullmatch(r"[A-Za-z0-9.-]+(?::([0-9]*))?", parsed.netloc)
            labels = host.removesuffix(".").split(".")
            if not all(
                re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label)
                for label in labels
            ):
                raise ValueError("Result URL requires valid URI host labels")
            if len(labels) > 1 and not labels[-1][0].isalpha():
                if (
                    host.endswith(".")
                    or len(labels) != 4
                    or not all(
                        label.isascii() and label.isdigit() and int(label) <= 255
                        for label in labels
                    )
                ):
                    raise ValueError("Result URL requires a domain or IPv4 host")
        # Java URI stores a nonnegative signed-int port; it does not enforce TCP limits.
        if authority is None or (
            authority.group(1) and int(authority.group(1)) > 2**31 - 1
        ):
            raise ValueError("Result URL has an invalid URI authority or port")
        value.encode("utf-8")
        return value

    @field_validator("filename")
    @classmethod
    def validate_filename(cls, value: str) -> str:
        if (
            _utf16_length(value) > 255
            or "/" in value
            or "\\" in value
            or any(
                ord(character) < 32 or 127 <= ord(character) <= 159
                for character in value
            )
        ):
            raise ValueError(
                "Invalid result filename length or path/control characters"
            )
        return value

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, value: str) -> str:
        if _utf16_length(value) > 128:
            raise ValueError("Result media type exceeds 128 characters")
        primary, _, tail = value.partition(";")
        parts = primary.strip(_JAVA_TRIM).split("/")
        if len(parts) != 2 or not all(re.fullmatch(_TOKEN, part) for part in parts):
            raise ValueError("Invalid result media type")
        if parts[0] == "*" or parts[1] == "*" or parts[1].startswith("*+"):
            raise ValueError("Result media type must be concrete")
        # Mirror Spring's quoted-semicolon handling and ignore bare parameters.
        parameters: dict[str, str] = {}
        quoted = False
        beginning = 0
        for index, character in enumerate(tail + ";"):
            if character == '"' and (index == 0 or tail[index - 1] != "\\"):
                quoted = not quoted
            if character == ";" and (not quoted or index == len(tail)):
                parameter = tail[beginning:index].strip(_JAVA_TRIM)
                if "=" in parameter:
                    name, parameter_value = parameter.split("=", 1)
                    parameters[name.strip(_JAVA_TRIM)] = parameter_value.strip(
                        _JAVA_TRIM
                    )
                beginning = index + 1
        for name, parameter_value in parameters.items():
            is_quoted = (
                len(parameter_value) >= 2
                and parameter_value[0] in "\"'"
                and parameter_value[-1] == parameter_value[0]
            )
            if not re.fullmatch(_TOKEN, name) or (
                not is_quoted and not re.fullmatch(_TOKEN, parameter_value)
            ):
                raise ValueError("Invalid media type parameter")
            unquoted = parameter_value[1:-1] if is_quoted else parameter_value
            if name == "q" and not 0 <= float(unquoted) <= 1:
                raise ValueError("Invalid media type quality factor")
        charset = next(
            (
                parameter
                for name, parameter in reversed(parameters.items())
                if name.lower() == "charset"
            ),
            None,
        )
        if charset is not None:
            if len(charset) >= 2 and charset[0] in "\"'" and charset[-1] == charset[0]:
                charset = charset[1:-1]
            try:
                codecs.lookup(charset)
            except LookupError as error:
                raise ValueError("Unsupported media type charset") from error

        return value


class GenerationSuccessOutcome(FrozenModel):
    status: Literal["SUCCEEDED"]
    sources: Annotated[tuple[ResultSource, ...], Field(min_length=1, max_length=64)]


class GenerationFailureOutcome(FrozenModel):
    status: Literal["FAILED"]
    error: GenerationError


GenerationOutcome = Annotated[
    GenerationSuccessOutcome | GenerationFailureOutcome, Field(discriminator="status")
]


class GenerationEvent(FrozenModel):
    schema_version: SchemaVersion = 2
    event_id: CanonicalUuid
    event_type: Literal["GENERATION_EVENT"] = "GENERATION_EVENT"
    occurred_at: UtcTimestamp
    user_id: CanonicalUuid
    session_id: CanonicalUuid
    task_id: CanonicalUuid
    sequence_no: PositiveSequence
    connector_status: ConnectorExecutionStatus
    outcome: GenerationOutcome | None

    @model_validator(mode="after")
    def validate_status_outcome(self) -> Self:
        if self.connector_status in ("ACCEPTED", "RUNNING"):
            if self.outcome is not None:
                raise ValueError("Progress observations require null outcome")
        elif self.outcome is None or self.outcome.status != self.connector_status:
            raise ValueError("Terminal status requires its matching outcome")
        return self


def _validate_json_value(value: JsonValue) -> None:
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Nonfinite numbers are not JSON values")
        return
    if type(value) is str:
        value.encode("utf-8")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            key.encode("utf-8")
            _validate_json_value(item)
        return
    raise ValueError("Expected a JSON value")


def canonical_json(value: JsonValue) -> str:
    """Preserve JSON scalar/array values while sorting keys for duplicate comparison."""
    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> JsonValue:
    raise ValueError(f"Invalid JSON numeric constant: {value}")


def decode_command(raw: bytes | str) -> GenerationSubmit | TaskStatusQuery:
    """Decode a bounded original command, rejecting lossy or ambiguous ingress."""
    if isinstance(raw, bytes):
        if len(raw) > MAX_ENVELOPE_BYTES:
            raise ValueError("Generation envelope exceeds 31 MiB")
        text = raw.decode("utf-8", errors="strict")
    elif isinstance(raw, str):
        if len(raw.encode("utf-8")) > MAX_ENVELOPE_BYTES:
            raise ValueError("Generation envelope exceeds 31 MiB")
        text = raw
    else:
        raise ValueError("Command envelope must be UTF-8 bytes or a string")
    value: JsonValue = json.loads(
        text, object_pairs_hook=_unique_object, parse_constant=_reject_constant
    )
    _validate_json_value(value)
    if not isinstance(value, dict):
        raise ValueError("Command envelope must be an object")
    match value.get("event_type"):
        case "SUBMIT":
            return GenerationSubmit.model_validate(value)
        case "TASK_STATUS_QUERY":
            return TaskStatusQuery.model_validate(value)
        case _:
            raise ValueError("Unsupported generation command event_type")


def encode_event(event: GenerationEvent) -> str:
    """Encode the complete immutable observation within the transport budget."""
    value = event.model_dump(mode="json", exclude_unset=True)
    value["schema_version"] = event.schema_version
    value["event_type"] = event.event_type
    encoded = canonical_json(value)
    if len(encoded.encode("utf-8")) > MAX_ENVELOPE_BYTES:
        raise ValueError("Generation envelope exceeds 31 MiB")
    return encoded
