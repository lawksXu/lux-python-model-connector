"""Single-dispatch generation and independent recovery of original provider tasks."""

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from lux_model_connector.byteplus import (
    MAX_PROVIDER_BODY_BYTES,
    BytePlusClient,
    BytePlusRejected,
    BytePlusUncertainSubmission,
    ProviderAPIError,
    ProviderSubmissionPayload,
    SeedanceReceipt,
    SeedanceRequest,
    SeedreamReceipt,
    SeedreamRequest,
)
from lux_model_connector.generation_plan import (
    AdapterRegistry,
    AssetLocator,
    ImageGenerationRequest,
    NormalizedAccepted,
    NormalizedPreparedSubmission,
    NormalizedReceipt,
    VideoGenerationRequest,
    decode_generation_request,
    validate_expectations,
)
from lux_model_connector.protocol import (
    GenerationError,
    GenerationFailureOutcome,
    GenerationSuccessOutcome,
    ProviderErrorDiagnostic,
)
from lux_model_connector.results import (
    PreparedOutput,
    ResultSourceUnavailable,
    S3ResultStore,
)
from lux_model_connector.store import ConnectorStore

if TYPE_CHECKING:
    from lux_model_connector.results import StoredResult
    from lux_model_connector.store import ExecutionWork

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PendingReceipt:
    user_id: str
    task_id: str
    external_id: str | None
    receipt: SeedreamReceipt | SeedanceReceipt | NormalizedReceipt | None


@dataclass(frozen=True)
class PendingFailure:
    user_id: str
    task_id: str
    error: GenerationError


def _provider_error(
    code: str, message: str, error: ProviderAPIError | None = None
) -> GenerationError:
    return GenerationError(
        code=code,
        stage="PROVIDER",
        message=message,
        provider_diagnostic=(
            ProviderErrorDiagnostic(
                provider_id="byteplus",
                provider_code=error.code[:256],
                provider_message=error.message[:4096],
            )
            if error is not None
            else None
        ),
    )


def _verify_saved_result(results: S3ResultStore, saved: StoredResult) -> None:
    # Reopening verifies the immutable object's metadata before result recovery.
    opened = results.read(saved)
    opened.body.close()


class ProviderWorker:
    def __init__(
        self,
        store: ConnectorStore,
        client: BytePlusClient,
        results: S3ResultStore,
        *,
        interval_seconds: float = 5.0,
        page_size: int = 32,
        adapters: AdapterRegistry | None = None,
        asset_locator: AssetLocator | None = None,
    ) -> None:
        if not 0 < interval_seconds <= 300 or not 1 <= page_size <= 128:
            raise ValueError("Provider polling interval and page size must be bounded")
        self._adapters = adapters or AdapterRegistry()
        self._asset_locator = asset_locator
        self._store = store
        self._client = client
        self._results = results
        self._interval_seconds = interval_seconds
        self._page_size = page_size
        self._cursor = 0
        self._pending: PendingReceipt | PendingFailure | None = None
        self._cycle = asyncio.Lock()

    async def step(self) -> None:
        async with self._cycle:
            # A response obtained from POST must survive a transient DB failure in
            # this process. Do not issue another paid request while it is pending.
            if not await self._persist_pending():
                return
            try:
                await self._submit_one()
            except Exception as error:
                _LOGGER.warning(
                    "Generation submission deferred (%s)", type(error).__name__
                )
            if self._pending is not None:
                return

            page = await asyncio.to_thread(
                self._store.scan_provider_work,
                after_id=self._cursor,
                limit=self._page_size,
            )
            self._cursor = page[-1].execution.id if len(page) == self._page_size else 0
            for work in page:
                try:
                    await self._observe(work)
                except Exception as error:
                    _LOGGER.warning(
                        "Generation observation deferred (%s)", type(error).__name__
                    )
                if self._pending is not None:
                    return

    async def _persist_pending(self) -> bool:
        pending = self._pending
        if pending is None:
            return True
        try:
            if isinstance(pending, PendingFailure):
                await asyncio.to_thread(
                    self._store.save_submission_failure,
                    pending.user_id,
                    pending.task_id,
                    pending.error,
                )
            else:
                await asyncio.to_thread(
                    self._store.save_provider_receipt,
                    pending.user_id,
                    pending.task_id,
                    pending.external_id,
                    pending.receipt,
                )
        except Exception as error:
            _LOGGER.warning(
                "Provider receipt persistence deferred (%s)", type(error).__name__
            )
            return False
        self._pending = None
        return True

    async def _submit_one(self) -> None:
        claim = await asyncio.to_thread(
            self._store.claim_execution,
            account_ref=self._client.settings.account_ref,
            normalized_routes=self._adapters.registrations,
        )
        if claim is None:
            return
        execution = claim.work.execution
        try:
            payload = claim.work.submission_payload
            if payload is None:
                try:
                    request = decode_generation_request(
                        execution.input_generation_payload.request.root, execution.kind
                    )
                    if isinstance(
                        request, (ImageGenerationRequest, VideoGenerationRequest)
                    ):
                        validate_expectations(request)
                        registration, adapter = self._adapters.resolve(request)
                        if (
                            request.parameters.references
                            and self._asset_locator is None
                        ):
                            raise ValueError("Authorized Asset locator unavailable")
                        assets = []
                        reference_bytes = 0
                        for reference in request.parameters.references:
                            asset = await self._asset_locator.locate(
                                execution.input_generation_payload.identity,
                                reference.asset_id,
                            )
                            if asset.asset_id != reference.asset_id:
                                raise ValueError(
                                    "Asset locator returned a different Asset"
                                )
                            reference_bytes += len(asset.url.encode("utf-8"))
                            if reference_bytes > MAX_PROVIDER_BODY_BYTES - 1024 * 1024:
                                raise ValueError(
                                    "Aggregate reference archive exceeds budget"
                                )
                            assets.append(asset)
                        translated = await adapter.translate(request, tuple(assets))
                        payload = NormalizedPreparedSubmission(
                            account_ref=registration.account_ref,
                            kind=execution.kind,
                            binding=request.provider_binding,
                            request=request,
                            provider_payload=translated,
                        )
                    else:
                        payload = ProviderSubmissionPayload(
                            account_ref=self._client.settings.account_ref,
                            kind=execution.kind,
                            request=request,
                        )
                except ValueError:
                    await asyncio.to_thread(
                        self._store.fail_execution,
                        claim,
                        GenerationError(
                            code=(
                                "INVALID_GENERATION_REQUEST"
                                if "request_type"
                                in execution.input_generation_payload.request.root
                                else "INVALID_BYTEPLUS_REQUEST"
                            ),
                            stage="VALIDATION",
                            message="No valid implemented execution route.",
                        ),
                    )
                    return
                if not await asyncio.to_thread(
                    self._store.prepare_submission, claim, payload
                ):
                    return
            elif (
                isinstance(payload, ProviderSubmissionPayload)
                and payload.account_ref != self._client.settings.account_ref
            ):
                return

            if isinstance(payload, NormalizedPreparedSubmission):
                registration, adapter = self._adapters.resolve(payload.request)
                if (
                    registration.account_ref != payload.account_ref
                    or registration.binding != payload.binding
                ):
                    return
            if not await asyncio.to_thread(self._store.begin_submission, claim):
                return
            try:
                if isinstance(payload, NormalizedPreparedSubmission):
                    registration, adapter = self._adapters.resolve(payload.request)
                    if (
                        registration.account_ref != payload.account_ref
                        or registration.binding != payload.binding
                    ):
                        return
                    receipt = await adapter.submit(
                        execution.input_generation_payload.identity, payload
                    )
                    if receipt is None:
                        return
                    pending = PendingReceipt(
                        execution.user_id,
                        execution.task_id,
                        receipt.external_id
                        if isinstance(receipt, NormalizedAccepted)
                        else None,
                        None if isinstance(receipt, NormalizedAccepted) else receipt,
                    )
                elif isinstance(payload.request, SeedreamRequest):
                    receipt = await self._client.generate_image(payload.request)
                    pending = PendingReceipt(
                        execution.user_id, execution.task_id, None, receipt
                    )
                else:
                    created = await self._client.create_video(payload.request)
                    pending = PendingReceipt(
                        execution.user_id, execution.task_id, created.id, None
                    )
            except BytePlusRejected as error:
                self._pending = PendingFailure(
                    execution.user_id,
                    execution.task_id,
                    _provider_error(
                        "BYTEPLUS_SUBMISSION_REJECTED",
                        "BytePlus rejected the generation submission.",
                        error.error,
                    ),
                )
                await self._persist_pending()
            except BytePlusUncertainSubmission:
                # The durable dispatch marker excludes this execution from a
                # future submission claim, including after worker restarts.
                _LOGGER.warning("BytePlus generation acceptance remains uncertain")
            else:
                self._pending = pending
                await self._persist_pending()
        finally:
            await asyncio.to_thread(self._store.release_execution, claim)

    async def _observe(self, work: ExecutionWork) -> None:
        payload = work.submission_payload
        if isinstance(payload, NormalizedPreparedSubmission):
            if work.submission_started_at is None:
                return
            registration, adapter = self._adapters.resolve(payload.request)
            if (
                registration.account_ref != payload.account_ref
                or registration.binding != payload.binding
            ):
                return
            if work.provider_receipt is None:
                receipt = await adapter.reconcile(
                    work.execution.input_generation_payload.identity,
                    payload,
                    work.provider_external_id,
                    work.provider_receipt,
                )
                if receipt is None:
                    return
                self._pending = PendingReceipt(
                    work.execution.user_id,
                    work.execution.task_id,
                    work.provider_external_id,
                    receipt,
                )
                if not await self._persist_pending():
                    return
                work = replace(work, provider_receipt=receipt)
            await self._preserve_and_complete(work)
            return
        if payload is None or payload.account_ref != self._client.settings.account_ref:
            return
        receipt = work.provider_receipt
        if work.execution.kind == "VIDEO":
            if not isinstance(receipt, SeedanceReceipt) or receipt.status in (
                "queued",
                "running",
            ):
                if work.provider_external_id is None:
                    return
                receipt = await self._client.get_video(work.provider_external_id)
                if receipt.status in ("queued", "running"):
                    return
                if receipt.status == "succeeded" and (
                    receipt.content is None
                    or receipt.content.video_url is None
                    or (
                        isinstance(payload.request, SeedanceRequest)
                        and payload.request.return_last_frame is True
                        and receipt.content.last_frame_url is None
                    )
                ):
                    # A malformed observation can be queried again; do not freeze
                    # an incomplete terminal receipt or manufacture success.
                    return
                self._pending = PendingReceipt(
                    work.execution.user_id,
                    work.execution.task_id,
                    work.provider_external_id,
                    receipt,
                )
                if not await self._persist_pending():
                    return
                work = replace(work, provider_receipt=receipt)
        if receipt is None:
            return
        await self._preserve_and_complete(work)

    async def _preserve_and_complete(self, work: ExecutionWork) -> None:
        claim = await asyncio.to_thread(self._store.claim_result, work)
        if claim is None:
            return
        execution = claim.work.execution
        try:
            receipt = claim.work.provider_receipt
            if isinstance(receipt, NormalizedReceipt):
                if (
                    receipt.status == "FAILED"
                    or len(receipt.outputs) != 1
                    or receipt.outputs[0].source_index != 0
                    or receipt.outputs[0].media_kind != execution.kind
                ):
                    await asyncio.to_thread(
                        self._store.complete_execution,
                        claim,
                        GenerationFailureOutcome(
                            status="FAILED",
                            error=receipt.error
                            or GenerationError(
                                code="NORMALIZED_RESULT_INVALID",
                                stage="RESULT_PRESERVATION",
                                message="Normalized output requires one primary file.",
                            ),
                        ),
                    )
                    return
                outputs = receipt.outputs
            elif isinstance(receipt, SeedreamReceipt):
                error = receipt.error or next(
                    (image.error for image in receipt.data if image.error is not None),
                    None,
                )
                if error is not None:
                    await asyncio.to_thread(
                        self._store.complete_execution,
                        claim,
                        GenerationFailureOutcome(
                            status="FAILED",
                            error=_provider_error(
                                "BYTEPLUS_IMAGE_FAILED",
                                "BytePlus did not complete image generation.",
                                error,
                            ),
                        ),
                    )
                    return
                outputs = tuple(
                    PreparedOutput(
                        source_index=index,
                        url=image.url,
                        b64_json=image.b64_json,
                        expected_format=image.output_format,
                        media_kind="IMAGE",
                    )
                    for index, image in enumerate(receipt.data)
                )
            elif isinstance(receipt, SeedanceReceipt):
                if receipt.status in ("cancelled", "failed", "expired"):
                    await asyncio.to_thread(
                        self._store.complete_execution,
                        claim,
                        GenerationFailureOutcome(
                            status="FAILED",
                            error=_provider_error(
                                f"BYTEPLUS_VIDEO_{receipt.status.upper()}",
                                f"BytePlus video generation {receipt.status}.",
                                receipt.error,
                            ),
                        ),
                    )
                    return
                if (
                    receipt.status != "succeeded"
                    or receipt.content is None
                    or receipt.content.video_url is None
                ):
                    return
                outputs = (
                    PreparedOutput(
                        source_index=0,
                        url=receipt.content.video_url,
                        expected_format=receipt.output_format,
                        media_kind="VIDEO",
                    ),
                )
                if receipt.content.last_frame_url is not None:
                    outputs += (
                        PreparedOutput(
                            source_index=1,
                            url=receipt.content.last_frame_url,
                            media_kind="IMAGE",
                        ),
                    )
            else:
                return
            saved = await asyncio.to_thread(
                self._store.saved_results, execution.user_id, execution.task_id
            )
            if len(saved) > len(outputs) or any(
                result.source_index != index for index, result in enumerate(saved)
            ):
                raise ValueError(
                    "Saved result manifest does not match the provider receipt"
                )
            for output in outputs:
                if not await asyncio.to_thread(self._store.renew_execution, claim):
                    return
                if output.source_index < len(saved):
                    await asyncio.to_thread(
                        _verify_saved_result, self._results, saved[output.source_index]
                    )
                    continue
                result = await self._results.preserve(
                    execution.user_id, execution.task_id, output
                )
                if not await asyncio.to_thread(self._store.save_result, claim, result):
                    return
            if isinstance(claim.work.submission_payload, NormalizedPreparedSubmission):
                payload = claim.work.submission_payload
                _, adapter = self._adapters.resolve(payload.request)
                verified = await asyncio.to_thread(
                    self._store.saved_results, execution.user_id, execution.task_id
                )
                for result in verified:
                    try:
                        await adapter.verify_result(payload.request, result)
                    except ValueError:
                        await asyncio.to_thread(
                            self._store.complete_execution,
                            claim,
                            GenerationFailureOutcome(
                                status="FAILED",
                                error=GenerationError(
                                    code="NORMALIZED_RESULT_INVALID",
                                    stage="RESULT_PRESERVATION",
                                    message="Result violates normalized expectations.",
                                ),
                            ),
                        )
                        return
            sources = await asyncio.to_thread(self._store.result_sources, claim)
            if len(sources) != len(outputs):
                return
            await asyncio.to_thread(
                self._store.complete_execution,
                claim,
                GenerationSuccessOutcome(status="SUCCEEDED", sources=sources),
            )
        except ResultSourceUnavailable:
            # A reclaimed worker may observe an expired provider URL while an
            # earlier object's PUT is still in flight. Its eventual stable slot
            # can recover the result, so URL absence alone is not terminal proof.
            _LOGGER.warning(
                "Original provider result unavailable; preservation remains uncertain"
            )
        finally:
            await asyncio.to_thread(self._store.release_execution, claim)

    async def run(self) -> None:
        while True:
            try:
                await self.step()
            except Exception as error:
                _LOGGER.warning("Provider cycle deferred (%s)", type(error).__name__)
            await asyncio.sleep(self._interval_seconds)
