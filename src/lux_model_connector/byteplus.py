"""Typed BytePlus ModelArk REST boundary; submissions are never retried here."""

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Annotated, Literal, Self

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SerializerFunctionWrapHandler,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
    model_serializer,
    model_validator,
)

from lux_model_connector.protocol import (
    FrozenModel,
    GenerationKind,
    NonblankString,
    canonical_json,
)

BYTEPLUS_BASE_URL = "https://ark.ap-southeast.bytepluses.com/api/v3"
MAX_PROVIDER_BODY_BYTES = 64 * 1024 * 1024


class ProviderRequest(FrozenModel):
    """Omitted options keep provider defaults; explicit null is not API input."""

    @model_validator(mode="before")
    @classmethod
    def reject_explicit_null(cls, value: JsonValue) -> JsonValue:
        if isinstance(value, dict) and any(item is None for item in value.values()):
            raise ValueError(
                "Provider request fields cannot be null; omit them instead"
            )
        return value

    @model_serializer(mode="wrap")
    def serialize_supplied_fields(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, JsonValue]:
        # Submission archives preserve omissions too, even inside an enclosing model.
        payload: dict[str, JsonValue] = handler(self)
        return {
            name: value
            for name, value in payload.items()
            if name in self.model_fields_set
        }

    def provider_payload(self) -> dict[str, JsonValue]:
        # This map exists only at the HTTP serialization boundary.
        payload: dict[str, JsonValue] = self.model_dump(mode="json", exclude_unset=True)
        payload["model"] = payload.pop("model_id")
        return payload


class OptimizePromptOptions(ProviderRequest):
    mode: Literal["standard", "fast"] | None = None


class SequentialImageGenerationOptions(ProviderRequest):
    max_images: Annotated[StrictInt, Field(ge=1, le=15)] | None = None


class SeedreamRequest(ProviderRequest):
    """Upstream Model ID is opaque; model-specific capabilities stay remote."""

    model_id: NonblankString
    prompt: NonblankString | None = None
    image: NonblankString | tuple[NonblankString, ...] | None = None
    layer_decomposition: StrictBool | None = None
    size: NonblankString | None = None
    optimize_prompt_options: OptimizePromptOptions | None = None
    output_format: Literal["png", "jpeg"] | None = None
    background: Literal["transparent", "opaque"] | None = None
    response_format: Literal["url", "b64_json"] | None = None
    sequential_image_generation: Literal["auto", "disabled"] | None = None
    sequential_image_generation_options: SequentialImageGenerationOptions | None = None
    stream: StrictBool | None = None
    watermark: StrictBool | None = None

    @model_validator(mode="after")
    def validate_scenario(self) -> Self:
        if self.stream:
            raise ValueError("Streaming image responses are not supported yet")
        if isinstance(self.image, tuple) and not self.image:
            raise ValueError("Input image list cannot be empty")
        if self.layer_decomposition:
            if self.image is None or (
                isinstance(self.image, tuple) and len(self.image) != 1
            ):
                raise ValueError("Layer decomposition requires exactly one image")
        elif self.prompt is None:
            raise ValueError("Image generation requires a prompt")
        return self


class ProviderMediaUrl(ProviderRequest):
    """BytePlus-owned URL, data URI, or digital-character asset URI input."""

    url: NonblankString


class SeedanceText(ProviderRequest):
    type: Literal["text"]
    text: NonblankString


class SeedanceImage(ProviderRequest):
    type: Literal["image_url"]
    image_url: ProviderMediaUrl
    role: Literal["first_frame", "last_frame", "reference_image", ""] | None = None


class SeedanceVideo(ProviderRequest):
    type: Literal["video_url"]
    video_url: ProviderMediaUrl
    role: Literal["reference_video"] | None = None


class SeedanceAudio(ProviderRequest):
    type: Literal["audio_url"]
    audio_url: ProviderMediaUrl
    role: Literal["reference_audio"] | None = None


def _external_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("Provider task ID must be a nonempty safe path segment")
    return value


class DraftTask(ProviderRequest):
    id: NonblankString


class SeedanceDraft(ProviderRequest):
    type: Literal["draft_task"]
    draft_task: DraftTask


SeedanceContent = Annotated[
    SeedanceText | SeedanceImage | SeedanceVideo | SeedanceAudio | SeedanceDraft,
    Field(discriminator="type"),
]


class SeedanceRequest(ProviderRequest):
    model_id: NonblankString
    content: Annotated[tuple[SeedanceContent, ...], Field(min_length=1)]
    omni_reference_task_type: Literal["auto", "reference", "edit", "extend"] | None = (
        None
    )
    resolution: NonblankString | None = None
    ratio: NonblankString | None = None
    duration: StrictInt | None = None
    frames: Annotated[StrictInt, Field(ge=1)] | None = None
    generate_audio: StrictBool | None = None
    watermark: StrictBool | None = None
    output_format: Literal["mp4", "mov"] | None = None
    seed: Annotated[StrictInt, Field(ge=-1, le=2147483647)] | None = None
    camera_fixed: StrictBool | None = None
    return_last_frame: StrictBool | None = None
    draft: StrictBool | None = None
    service_tier: Literal["default", "flex"] | None = None
    callback_url: NonblankString | None = None
    execution_expires_after: Annotated[StrictInt, Field(ge=3600, le=259200)] | None = (
        None
    )
    priority: Annotated[StrictInt, Field(ge=0, le=9)] | None = None
    safety_identifier: (
        Annotated[StrictStr, Field(min_length=1, max_length=64)] | None
    ) = None

    @model_validator(mode="after")
    def validate_duration(self) -> Self:
        if self.duration is not None and self.duration != -1 and self.duration < 1:
            raise ValueError("Duration must be positive or -1")
        return self


class ProviderSubmissionPayload(FrozenModel):
    """Exact request/account archive, with credentials resolved only at runtime."""

    provider_id: Literal["byteplus"] = "byteplus"
    account_ref: NonblankString = "byteplus-primary"
    kind: GenerationKind
    request: SeedreamRequest | SeedanceRequest

    @model_validator(mode="after")
    def validate_request_kind(self) -> Self:
        if (self.kind == "IMAGE") != isinstance(self.request, SeedreamRequest):
            raise ValueError("Submission kind does not match its typed request")
        return self


class ProviderResponse(BaseModel):
    """Known interpreted fields are typed; unknown future metadata is ignored."""

    model_config = ConfigDict(frozen=True, extra="ignore")


class ProviderAPIError(ProviderResponse):
    code: NonblankString
    message: NonblankString


class ProviderErrorResponse(ProviderResponse):
    error: ProviderAPIError


class LayerBoundingBox(ProviderResponse):
    absolute: tuple[StrictInt, StrictInt, StrictInt, StrictInt] | None = None
    normalized: tuple[StrictInt, StrictInt, StrictInt, StrictInt] | None = None


class SeedreamImage(ProviderResponse):
    url: NonblankString | None = None
    b64_json: NonblankString | None = None
    size: NonblankString | None = None
    output_format: Literal["png", "jpeg"] | None = None
    z_index: StrictInt | None = None
    name: StrictStr | None = None
    description: StrictStr | None = None
    bounding_box: LayerBoundingBox | None = None
    error: ProviderAPIError | None = None

    @model_validator(mode="after")
    def validate_output(self) -> Self:
        if self.url is None and self.b64_json is None and self.error is None:
            raise ValueError("Image item must contain output or failure evidence")
        return self


class SeedreamUsage(ProviderResponse):
    generated_images: StrictInt | None = None
    input_images: StrictInt | None = None
    output_tokens: StrictInt | None = None
    total_tokens: StrictInt | None = None


class SeedreamReceipt(ProviderResponse):
    created: StrictInt | None = None
    model: StrictStr | None = None
    data: Annotated[tuple[SeedreamImage, ...], Field(max_length=64)] = ()
    error: ProviderAPIError | None = None
    usage: SeedreamUsage | None = None

    @model_validator(mode="after")
    def validate_response(self) -> Self:
        if not self.data and self.error is None:
            raise ValueError("Image response must contain output or failure evidence")
        return self


class VideoCreated(ProviderResponse):
    id: NonblankString

    @model_validator(mode="after")
    def validate_id(self) -> Self:
        _external_id(self.id)
        return self


class SeedanceOutput(ProviderResponse):
    video_url: NonblankString | None = None
    last_frame_url: NonblankString | None = None


class SeedanceUsage(ProviderResponse):
    completion_tokens: StrictInt | None = None
    total_tokens: StrictInt | None = None


class SeedanceReceipt(ProviderResponse):
    id: NonblankString
    status: Literal["queued", "running", "cancelled", "succeeded", "failed", "expired"]
    model: StrictStr | None = None
    content: SeedanceOutput | None = None
    error: ProviderAPIError | None = None
    created_at: StrictInt | None = None
    updated_at: StrictInt | None = None
    seed: StrictInt | None = None
    resolution: StrictStr | None = None
    ratio: StrictStr | None = None
    duration: StrictInt | None = None
    frames: StrictInt | None = None
    framespersecond: StrictInt | None = None
    generate_audio: StrictBool | None = None
    output_format: Literal["mp4", "mov"] | None = None
    safety_identifier: StrictStr | None = None
    priority: StrictInt | None = None
    draft: StrictBool | None = None
    draft_task_id: StrictStr | None = None
    service_tier: StrictStr | None = None
    execution_expires_after: StrictInt | None = None
    usage: SeedanceUsage | None = None


@dataclass(frozen=True)
class BytePlusSettings:
    api_key: str = field(repr=False)
    account_ref: str = "byteplus-primary"
    timeout_seconds: float = 600.0
    query_timeout_seconds: float = 30.0
    max_response_bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        if not isinstance(self.api_key, str) or not self.api_key.strip():
            raise ValueError("BytePlus API key is required")
        if any(
            ord(character) < 33 or ord(character) > 126 for character in self.api_key
        ):
            raise ValueError("BytePlus API key must be a valid header credential")
        if not isinstance(self.account_ref, str) or not self.account_ref.strip():
            raise ValueError("BytePlus account reference is required")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not 0 < self.timeout_seconds <= 1800
        ):
            raise ValueError("BytePlus timeout must be between 0 and 1800 seconds")
        if (
            type(self.query_timeout_seconds) not in (int, float)
            or not 0 < self.query_timeout_seconds <= 120
        ):
            raise ValueError("BytePlus query timeout must be between 0 and 120 seconds")
        if (
            type(self.max_response_bytes) is not int
            or not 0 < self.max_response_bytes <= 128 * 1024 * 1024
        ):
            raise ValueError("BytePlus response limit must be within 128 MiB")


class BytePlusRejected(Exception):
    """A definitive response says that this submission was not accepted."""

    def __init__(self, error: ProviderAPIError, status_code: int | None = None) -> None:
        self.error = error
        self.status_code = status_code
        super().__init__("BytePlus rejected the generation submission")


class BytePlusUncertainSubmission(Exception):
    """No safe proof of acceptance/nonacceptance; repeating POST is forbidden."""


class BytePlusQueryError(Exception):
    """Observation failure leaves the original generation running."""

    def __init__(
        self, error: ProviderAPIError | None = None, status_code: int | None = None
    ) -> None:
        self.error = error
        self.status_code = status_code
        super().__init__("BytePlus task observation was unavailable or invalid")


def _json_object(raw: bytes) -> JsonValue:
    def reject_duplicates(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate provider JSON field")
            result[key] = value
        return result

    def reject_nonfinite(value: str) -> None:
        raise ValueError("Nonfinite provider JSON number")

    value: JsonValue = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_nonfinite,
    )
    if not isinstance(value, dict):
        raise ValueError("Provider response must be an object")
    canonical_json(value)
    return value


class BytePlusClient:
    def __init__(
        self, settings: BytePlusSettings, http_client: httpx.AsyncClient
    ) -> None:
        self.settings = settings
        self.http_client = http_client

    async def generate_image(self, request: SeedreamRequest) -> SeedreamReceipt:
        raw = await self._request("POST", "/images/generations", request)
        try:
            return SeedreamReceipt.model_validate(_json_object(raw))
        except ValueError, UnicodeError:
            raise BytePlusUncertainSubmission(
                "BytePlus image receipt was invalid"
            ) from None

    async def create_video(self, request: SeedanceRequest) -> VideoCreated:
        raw = await self._request("POST", "/contents/generations/tasks", request)
        try:
            return VideoCreated.model_validate(_json_object(raw))
        except ValueError, UnicodeError:
            raise BytePlusUncertainSubmission(
                "BytePlus video receipt was invalid"
            ) from None

    async def get_video(self, external_id: str) -> SeedanceReceipt:
        _external_id(external_id)
        raw = await self._request("GET", f"/contents/generations/tasks/{external_id}")
        try:
            receipt = SeedanceReceipt.model_validate(_json_object(raw))
            if receipt.id != external_id:
                raise ValueError("Provider returned a different task")
            return receipt
        except ValueError, UnicodeError:
            raise BytePlusQueryError() from None

    async def _request(
        self,
        method: Literal["POST", "GET"],
        path: str,
        request: SeedreamRequest | SeedanceRequest | None = None,
    ) -> bytes:
        content = None
        if request is not None:
            content = canonical_json(request.provider_payload()).encode("utf-8")
            if len(content) > MAX_PROVIDER_BODY_BYTES:
                raise ValueError("Provider request exceeds 64 MiB")
        timeout = (
            self.settings.query_timeout_seconds
            if method == "GET"
            else self.settings.timeout_seconds
        )
        try:
            async with asyncio.timeout(timeout):
                async with self.http_client.stream(
                    method,
                    f"{BYTEPLUS_BASE_URL}{path}",
                    content=content,
                    headers={
                        "Authorization": f"Bearer {self.settings.api_key}",
                        "Content-Type": "application/json",
                    },
                    timeout=httpx.Timeout(timeout),
                    follow_redirects=False,
                ) as response:
                    chunks = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(chunks) + len(chunk) > self.settings.max_response_bytes:
                            raise ValueError(
                                "Provider response exceeds its read budget"
                            )
                        chunks.extend(chunk)
                    raw = bytes(chunks)
                    if 200 <= response.status_code < 300:
                        return raw
                    error = self._response_error(raw, response.status_code)
                    if method == "GET":
                        raise BytePlusQueryError(error, response.status_code)
                    if (
                        400 <= response.status_code < 500
                        and response.status_code != 408
                    ):
                        raise BytePlusRejected(error, response.status_code)
                    raise BytePlusUncertainSubmission(
                        "BytePlus submission did not return definitive acceptance"
                    )
        except httpx.HTTPError, TimeoutError, ValueError, UnicodeError:
            if method == "GET":
                raise BytePlusQueryError() from None
            raise BytePlusUncertainSubmission(
                "BytePlus submission acceptance could not be confirmed"
            ) from None

    @staticmethod
    def _response_error(raw: bytes, status_code: int) -> ProviderAPIError:
        try:
            return ProviderErrorResponse.model_validate(_json_object(raw)).error
        except ValidationError, ValueError, UnicodeError:
            return ProviderAPIError(
                code=f"HTTP_{status_code}", message="BytePlus returned an HTTP error"
            )
