"""Exact opt-in normalized BytePlus routes and provider-owned translation."""

import asyncio
import math
from dataclasses import dataclass

from pydantic import JsonValue

from lux_model_connector.byteplus import (
    MAX_PROVIDER_BODY_BYTES,
    BytePlusClient,
    ProviderMediaUrl,
    SeedanceImage,
    SeedanceReceipt,
    SeedanceRequest,
    SeedanceText,
    SeedreamReceipt,
    SeedreamRequest,
)
from lux_model_connector.generation_plan import (
    AdapterRegistration,
    AdapterRegistry,
    BytePlusImageReceiptMetadata,
    ImageGenerationRequest,
    LocatedAsset,
    ModelSelection,
    NormalizedAccepted,
    NormalizedGenerationRequest,
    NormalizedPreparedSubmission,
    NormalizedReceipt,
    PixelDimensions,
    PixelSize,
    ProviderBinding,
    VideoGenerationRequest,
    validate_expectations,
)
from lux_model_connector.media_inspection import (
    InspectedMedia,
    inspect_media,
    verify_content,
)
from lux_model_connector.protocol import (
    GenerationError,
    GenerationIdentity,
    GenerationKind,
    canonical_json,
)
from lux_model_connector.results import PreparedOutput, S3ResultStore, StoredResult


@dataclass(frozen=True)
class BytePlusModelSpec:
    model_id: str
    provider_model_id: str
    media_type: GenerationKind
    max_reference_count: int
    max_reference_bytes: int


MODEL_SPECS: tuple[BytePlusModelSpec, ...] = (
    BytePlusModelSpec(
        "seedream-4.5", "seedream-4-5-251128", "IMAGE", 14, 3 * 1024 * 1024
    ),
    BytePlusModelSpec(
        "seedream-5.0-lite", "seedream-5-0-260128", "IMAGE", 14, 3 * 1024 * 1024
    ),
    BytePlusModelSpec(
        "seedream-5.0-pro", "dola-seedream-5-0-pro-260628", "IMAGE", 10, 4 * 1024 * 1024
    ),
    BytePlusModelSpec(
        "seedance-2.5",
        "dreamina-seedance-2-5-260628",
        "VIDEO",
        30,
        3 * 1024 * 1024 // 2,
    ),
)
VIDEO_PRESETS: dict[tuple[int, int], PixelDimensions] = {
    (16, 9): PixelDimensions(width=1280, height=720),
    (9, 16): PixelDimensions(width=720, height=1280),
    (1, 1): PixelDimensions(width=960, height=960),
}


def _dimensions(request: NormalizedGenerationRequest) -> PixelDimensions:
    size = request.parameters.size
    return (
        size.dimensions if isinstance(size, PixelSize) else size.preset.pixel_dimensions
    )


def _reference_urls(
    request: NormalizedGenerationRequest, assets: tuple[LocatedAsset, ...]
) -> tuple[str, ...]:
    specification = next(
        (
            model
            for model in MODEL_SPECS
            if model.model_id == request.model_selection.model_id
        ),
        None,
    )
    if specification is None:
        raise ValueError("Unknown BytePlus model")
    if (
        len(assets) != len(request.parameters.references)
        or len(assets) > specification.max_reference_count
    ):
        raise ValueError("Unsupported reference count")
    for reference, asset in zip(request.parameters.references, assets, strict=True):
        if asset.asset_id != reference.asset_id or asset.media_type not in (
            "image/png",
            "image/jpeg",
        ):
            raise ValueError("Only owned PNG/JPEG reference images are supported")
        if (
            asset.size_bytes is None
            or asset.size_bytes > specification.max_reference_bytes
        ):
            raise ValueError("Reference byte length exceeds model-specific limit")
        if isinstance(request, VideoGenerationRequest):
            dimensions = asset.dimensions
            if dimensions is None or not (
                300 <= dimensions.width <= 6000 and 300 <= dimensions.height <= 6000
            ):
                raise ValueError("Seedance reference dimensions must be 300..6000")
            if not 0.4 <= dimensions.width / dimensions.height <= 2.5:
                raise ValueError("Seedance reference ratio must be 0.4..2.5")
        if not asset.url.startswith(f"data:{asset.media_type};base64,"):
            raise ValueError("Reference transport requires verified image data URI")
    return tuple(asset.url for asset in assets)


def normalize_seedream_receipt(
    request: SeedreamRequest, receipt: SeedreamReceipt
) -> NormalizedReceipt:
    """One immutable normalization for original submission and operator evidence."""
    metadata = BytePlusImageReceiptMetadata(
        created=receipt.created,
        model=receipt.model,
        usage=receipt.usage,
        error=receipt.error,
    )
    failure_code: str | None = None
    if receipt.error is not None or any(
        image.error is not None for image in receipt.data
    ):
        failure_code = "BYTEPLUS_IMAGE_FAILED"
    elif len(receipt.data) != 1 or (
        (receipt.data[0].url is None) == (receipt.data[0].b64_json is None)
    ):
        failure_code = "NORMALIZED_RESULT_INVALID"
    if failure_code is not None:
        return NormalizedReceipt(
            status="FAILED",
            original_image_receipt=metadata,
            error=GenerationError(
                code=failure_code,
                stage="PROVIDER",
                message="BytePlus output violated single-file generation expectations.",
            ),
        )
    image = receipt.data[0]
    return NormalizedReceipt(
        status="SUCCEEDED",
        original_image_receipt=metadata,
        outputs=(
            PreparedOutput(
                source_index=0,
                media_kind="IMAGE",
                url=image.url,
                b64_json=image.b64_json,
                expected_format=request.output_format,
            ),
        ),
    )


class BytePlusAdapter:
    def __init__(self, client: BytePlusClient, results: S3ResultStore) -> None:
        self._client, self._results = client, results

    async def translate(
        self, request: NormalizedGenerationRequest, assets: tuple[LocatedAsset, ...]
    ) -> dict[str, JsonValue]:
        validate_expectations(request)
        if len(request.parameters.prompt.encode("utf-8")) > 4000:
            raise ValueError("Normalized prompt exceeds 4000 UTF-8 bytes")
        urls = _reference_urls(request, assets)
        dimensions = _dimensions(request)
        if isinstance(request, ImageGenerationRequest):
            model = request.model_selection.model_id
            if model == "seedream-4.5" and request.parameters.output_format != "JPEG":
                raise ValueError("Seedream 4.5 only supports JPEG output format")
            allowed = (
                (1024, 1536, 2048) if model == "seedream-5.0-pro" else (2048, 4096)
            )
            if dimensions.width != dimensions.height or dimensions.width not in allowed:
                raise ValueError("Unsupported finite image dimensions")
            options: dict[str, JsonValue] = {
                "model_id": request.provider_binding.provider_model_id,
                "prompt": request.parameters.prompt,
                "size": f"{dimensions.width}x{dimensions.height}",
                "response_format": "url",
                "stream": False,
            }
            if model != "seedream-4.5":
                options["output_format"] = request.parameters.output_format.lower()
            if urls:
                options["image"] = list(urls)
            if model == "seedream-5.0-pro":
                options["layer_decomposition"] = False
            else:
                options["sequential_image_generation"] = "disabled"
            payload = SeedreamRequest.model_validate(options).model_dump(
                mode="json", exclude_unset=True
            )
        else:
            preset = request.parameters.size.preset
            ratio = (preset.aspect_ratio.width_units, preset.aspect_ratio.height_units)
            if preset.resolution_id != "720p" or VIDEO_PRESETS.get(ratio) != dimensions:
                raise ValueError("Unsupported finite video preset")
            if not 4 <= request.parameters.duration_seconds <= 30:
                raise ValueError("Seedance duration must be 4..30 seconds")
            content = (
                SeedanceText(type="text", text=request.parameters.prompt),
            ) + tuple(
                SeedanceImage(
                    type="image_url",
                    image_url=ProviderMediaUrl(url=url),
                    role="reference_image",
                )
                for url in urls
            )
            payload = SeedanceRequest(
                model_id=request.provider_binding.provider_model_id,
                content=content,
                resolution="720p",
                ratio=f"{ratio[0]}:{ratio[1]}",
                duration=request.parameters.duration_seconds,
                generate_audio=request.parameters.generate_audio,
                output_format=request.parameters.output_format.lower(),
                omni_reference_task_type="reference",
                draft=False,
                return_last_frame=False,
            ).model_dump(mode="json", exclude_unset=True)
        if len(canonical_json(payload).encode()) > MAX_PROVIDER_BODY_BYTES:
            raise ValueError(
                "Aggregate provider references exceed 64 MiB request budget"
            )
        return payload

    def _decode(
        self, prepared: NormalizedPreparedSubmission
    ) -> SeedreamRequest | SeedanceRequest:
        if (
            prepared.account_ref != self._client.settings.account_ref
            or prepared.binding != prepared.request.provider_binding
        ):
            raise ValueError("Prepared submission account or adapter binding differs")
        typed = (
            SeedreamRequest if prepared.kind == "IMAGE" else SeedanceRequest
        ).model_validate(prepared.provider_payload)
        if typed.model_id != prepared.binding.provider_model_id:
            raise ValueError("Prepared provider model differs")
        return typed

    async def submit(
        self, identity: GenerationIdentity, prepared: NormalizedPreparedSubmission
    ) -> NormalizedReceipt | NormalizedAccepted:
        request = self._decode(prepared)
        if isinstance(request, SeedanceRequest):
            created = await self._client.create_video(request)
            return NormalizedAccepted(external_id=created.id)
        receipt = await self._client.generate_image(request)
        return normalize_seedream_receipt(request, receipt)

    async def reconcile(
        self,
        identity: GenerationIdentity,
        prepared: NormalizedPreparedSubmission,
        external_id: str | None,
        receipt: NormalizedReceipt | None,
    ) -> NormalizedReceipt | None:
        self._decode(prepared)
        if receipt is not None:
            return receipt
        if prepared.kind != "VIDEO" or external_id is None:
            return None
        observed = await self._client.get_video(external_id)
        if observed.status in ("queued", "running"):
            return None
        if observed.status != "succeeded":
            return self._failed(
                f"BYTEPLUS_VIDEO_{observed.status.upper()}", video_receipt=observed
            )
        if observed.content is None or observed.content.video_url is None:
            return None
        if observed.content.last_frame_url is not None:
            return self._failed("NORMALIZED_RESULT_INVALID", video_receipt=observed)
        return NormalizedReceipt(
            status="SUCCEEDED",
            original_video_receipt=observed,
            outputs=(
                PreparedOutput(
                    source_index=0,
                    media_kind="VIDEO",
                    url=observed.content.video_url,
                    expected_format=prepared.request.parameters.output_format.lower(),
                ),
            ),
        )

    @staticmethod
    def _failed(
        code: str,
        *,
        video_receipt: SeedanceReceipt,
    ) -> NormalizedReceipt:
        return NormalizedReceipt(
            status="FAILED",
            original_video_receipt=video_receipt,
            error=GenerationError(
                code=code,
                stage="PROVIDER",
                message="BytePlus output violated single-file generation expectations.",
            ),
        )

    async def verify_result(
        self, request: NormalizedGenerationRequest, result: StoredResult
    ) -> None:
        def inspect() -> InspectedMedia:
            opened = self._results.read(result)
            try:
                body = bytearray()
                while chunk := opened.body.read(65536):
                    if len(body) + len(chunk) > result.byte_length:
                        raise ValueError("Retained byte length differs")
                    body.extend(chunk)
            finally:
                opened.body.close()
            verify_content(bytes(body), result.byte_length, result.sha256)
            return inspect_media(bytes(body), result.media_type)

        media = await asyncio.to_thread(inspect)
        dimensions = _dimensions(request)
        if media.output_format != request.parameters.output_format:
            raise ValueError("Retained output format differs")
        if (media.width, media.height) != (dimensions.width, dimensions.height):
            raise ValueError("Retained dimensions differ")
        if isinstance(request, VideoGenerationRequest):
            duration = media.duration_seconds
            if (
                duration is None
                or not math.isfinite(duration)
                or abs(duration - request.parameters.duration_seconds) * 1000
                > request.result_expectations.duration_tolerance_milliseconds
            ):
                raise ValueError("Retained video duration differs")
            if media.has_audio != request.parameters.generate_audio:
                raise ValueError("Retained video audio presence differs")


def byteplus_registry(
    client: BytePlusClient, results: S3ResultStore
) -> AdapterRegistry:
    registry, adapter = AdapterRegistry(), BytePlusAdapter(client, results)
    for specification in MODEL_SPECS:
        kind = specification.media_type
        registry.register(
            AdapterRegistration(
                binding=ProviderBinding(
                    provider_id="byteplus",
                    provider_model_id=specification.provider_model_id,
                    adapter_id="byteplus-seedance-v1"
                    if kind == "VIDEO"
                    else "byteplus-seedream-v1",
                ),
                account_ref=client.settings.account_ref,
                media_type=kind,
                model_selection=ModelSelection(
                    model_id=specification.model_id,
                    profile_id="standard",
                    capability_revision=1,
                ),
                single_file=True,
                auxiliary_outputs_disabled=True,
                result_verification=True,
            ),
            adapter,
        )
    return registry
