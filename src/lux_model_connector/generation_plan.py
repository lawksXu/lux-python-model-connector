"""Closed normalized generation contract and explicitly registered execution routes."""

from __future__ import annotations

from typing import Annotated, Literal, Protocol

from pydantic import (
    BeforeValidator,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    StrictStr,
    TypeAdapter,
    field_validator,
)

from lux_model_connector.byteplus import (
    ProviderAPIError,
    SeedanceReceipt,
    SeedreamUsage,
)
from lux_model_connector.protocol import (
    CanonicalUuid,
    FrozenModel,
    GenerationError,
    GenerationIdentity,
    GenerationKind,
)
from lux_model_connector.results import PreparedOutput, StoredResult


def _one(value: JsonValue) -> int:
    if type(value) is not int or value != 1:
        raise ValueError("Expected integer one")
    return value


IntegerOne = Annotated[Literal[1], BeforeValidator(_one)]


class ModelSelection(FrozenModel):
    model_id: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    profile_id: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    capability_revision: Annotated[StrictInt, Field(ge=1, le=9223372036854775807)]


class ProviderBinding(FrozenModel):
    provider_id: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    provider_model_id: Annotated[StrictStr, Field(min_length=1, max_length=255)]
    adapter_id: Annotated[StrictStr, Field(min_length=1, max_length=128)]

    @field_validator("provider_id", "provider_model_id", "adapter_id")
    @classmethod
    def nonblank_binding(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Provider binding fields must be nonblank")
        return value


class AspectRatio(FrozenModel):
    width_units: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    height_units: Annotated[StrictInt, Field(ge=1, le=2147483647)]


class PixelDimensions(FrozenModel):
    width: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    height: Annotated[StrictInt, Field(ge=1, le=2147483647)]


class SizePreset(FrozenModel):
    preset_id: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    aspect_ratio: AspectRatio
    resolution_id: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    pixel_dimensions: PixelDimensions


class ResolvedPresetSize(FrozenModel):
    type: Literal["resolved_preset_size"]
    preset: SizePreset


class PixelSize(FrozenModel):
    type: Literal["pixel_size"]
    dimensions: PixelDimensions


ExecutableSize = Annotated[ResolvedPresetSize | PixelSize, Field(discriminator="type")]

Uuid = CanonicalUuid


class ReferenceAsset(FrozenModel):
    type: Literal["reference_asset"]
    asset_id: Uuid


ImageOutputFormat = Literal["PNG", "JPEG"]

ImageMode = Literal["IMAGE"]


class ImageExecutionParameters(FrozenModel):
    type: Literal["image_execution_parameters"]
    prompt: Annotated[StrictStr, Field(min_length=1, max_length=64000)]
    size: ExecutableSize
    references: Annotated[
        tuple[ReferenceAsset, ...], Field(min_length=0, max_length=64)
    ]
    output_format: ImageOutputFormat
    generation_mode: ImageMode


class ImageResultExpectations(FrozenModel):
    type: Literal["image_result_expectations"]
    file_count: IntegerOne
    size: ExecutableSize
    output_format: ImageOutputFormat


class ImageGenerationRequest(FrozenModel):
    request_type: Literal["image_generation_plan_v1"]
    schema_version: IntegerOne
    model_selection: ModelSelection
    provider_binding: ProviderBinding
    parameters: ImageExecutionParameters
    result_expectations: ImageResultExpectations


VideoOutputFormat = Literal["MP4", "MOV"]

VideoMode = Literal["VIDEO"]


class VideoExecutionParameters(FrozenModel):
    type: Literal["video_execution_parameters"]
    prompt: Annotated[StrictStr, Field(min_length=1, max_length=64000)]
    size: ResolvedPresetSize
    references: Annotated[
        tuple[ReferenceAsset, ...], Field(min_length=0, max_length=64)
    ]
    output_format: VideoOutputFormat
    duration_seconds: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    generate_audio: StrictBool
    generation_mode: VideoMode


class VideoResultExpectations(FrozenModel):
    type: Literal["video_result_expectations"]
    file_count: IntegerOne
    size: ResolvedPresetSize
    output_format: VideoOutputFormat
    duration_seconds: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    duration_tolerance_milliseconds: Annotated[
        StrictInt, Field(ge=0, le=9223372036854775807)
    ]
    expected_audio: StrictBool


class VideoGenerationRequest(FrozenModel):
    request_type: Literal["video_generation_plan_v1"]
    schema_version: IntegerOne
    model_selection: ModelSelection
    provider_binding: ProviderBinding
    parameters: VideoExecutionParameters
    result_expectations: VideoResultExpectations


NormalizedGenerationRequest = Annotated[
    ImageGenerationRequest | VideoGenerationRequest, Field(discriminator="request_type")
]


def decode_generation_request(value: dict[str, JsonValue], kind: GenerationKind):
    from lux_model_connector.byteplus import SeedanceRequest, SeedreamRequest

    if "request_type" in value:
        request = TypeAdapter(NormalizedGenerationRequest).validate_python(value)
        if request.parameters.generation_mode != kind:
            raise ValueError("Normalized request media differs from execution")
        return request
    return (SeedreamRequest if kind == "IMAGE" else SeedanceRequest).model_validate(
        value
    )


class LocatedAsset(FrozenModel):
    asset_id: CanonicalUuid
    url: StrictStr
    media_type: StrictStr
    dimensions: PixelDimensions | None = None
    size_bytes: Annotated[StrictInt, Field(ge=1, le=10 * 1024 * 1024)] | None = None


class AssetLocator(Protocol):
    async def locate(
        self, identity: GenerationIdentity, asset_id: str
    ) -> LocatedAsset: ...


class NormalizedPreparedSubmission(FrozenModel):
    payload_type: Literal["normalized_prepared_v1"] = "normalized_prepared_v1"
    account_ref: StrictStr
    kind: GenerationKind
    binding: ProviderBinding
    request: NormalizedGenerationRequest
    # Provider archive is opaque here; the registered adapter owns its schema.
    provider_payload: dict[str, JsonValue]


class NormalizedAccepted(FrozenModel):
    """Submission evidence; worker saves the original ID before polling."""

    external_id: Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9_-]{1,1024}$")]


class BytePlusImageReceiptMetadata(FrozenModel):
    """Auditable provider metadata; primary bytes are archived only in outputs."""

    created: StrictInt | None = None
    model: StrictStr | None = None
    usage: SeedreamUsage | None = None
    error: ProviderAPIError | None = None


class NormalizedReceipt(FrozenModel):
    receipt_type: Literal["normalized_receipt_v1"] = "normalized_receipt_v1"
    status: Literal["SUCCEEDED", "FAILED"]
    outputs: tuple[PreparedOutput, ...] = ()
    error: GenerationError | None = None
    original_image_receipt: BytePlusImageReceiptMetadata | None = None
    original_video_receipt: SeedanceReceipt | None = None


NormalizedReceipt.model_rebuild()


class GenerationAdapter(Protocol):
    async def translate(
        self, request: NormalizedGenerationRequest, assets: tuple[LocatedAsset, ...]
    ) -> dict[str, JsonValue]: ...
    async def submit(
        self, identity: GenerationIdentity, prepared: NormalizedPreparedSubmission
    ) -> NormalizedReceipt | NormalizedAccepted | None: ...
    async def reconcile(
        self,
        identity: GenerationIdentity,
        prepared: NormalizedPreparedSubmission,
        external_id: str | None,
        receipt: NormalizedReceipt | None,
    ) -> NormalizedReceipt | None: ...
    async def verify_result(
        self, request: NormalizedGenerationRequest, result: StoredResult
    ) -> None: ...


class AdapterRegistration(FrozenModel):
    binding: ProviderBinding
    account_ref: StrictStr
    media_type: GenerationKind
    model_selection: ModelSelection
    schema_version: IntegerOne = 1
    single_file: Literal[True]
    auxiliary_outputs_disabled: Literal[True]
    result_verification: Literal[True]


class AdapterRegistry:
    def __init__(self) -> None:
        self._entries: list[tuple[AdapterRegistration, GenerationAdapter]] = []

    @property
    def registrations(self) -> tuple[AdapterRegistration, ...]:
        return tuple(entry[0] for entry in self._entries)

    def register(
        self, registration: AdapterRegistration, adapter: GenerationAdapter
    ) -> None:
        # One exact routing selector resolves one account, including on recovery.
        if any(
            existing.binding == registration.binding
            and existing.model_selection == registration.model_selection
            and existing.media_type == registration.media_type
            and existing.schema_version == registration.schema_version
            for existing, _ in self._entries
        ):
            raise ValueError("Duplicate adapter registration")
        self._entries.append((registration, adapter))

    def resolve(
        self, request: NormalizedGenerationRequest
    ) -> tuple[AdapterRegistration, GenerationAdapter]:
        for registration, adapter in self._entries:
            if (
                registration.binding == request.provider_binding
                and registration.model_selection == request.model_selection
                and registration.media_type == request.parameters.generation_mode
                and registration.schema_version == request.schema_version
            ):
                return registration, adapter
        raise ValueError(
            "No verified adapter for normalized binding/model/profile/revision"
        )


def validate_expectations(request: NormalizedGenerationRequest) -> None:
    parameters, expected = request.parameters, request.result_expectations
    if (
        parameters.size != expected.size
        or parameters.output_format != expected.output_format
    ):
        raise ValueError("Output expectations disagree with execution parameters")
    if isinstance(request, VideoGenerationRequest) and (
        parameters.duration_seconds != expected.duration_seconds
        or parameters.generate_audio != expected.expected_audio
    ):
        raise ValueError("Video expectations disagree with execution parameters")
