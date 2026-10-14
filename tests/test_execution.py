import asyncio
import io
import threading
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from lux_model_connector.byteplus import (
        ProviderSubmissionPayload,
        SeedanceReceipt,
        SeedreamReceipt,
    )
    from lux_model_connector.protocol import GenerationError, GenerationOutcome
    from lux_model_connector.results import PreparedOutput, StoredResult
    from lux_model_connector.store import ExecutionClaim, ExecutionWork


def initial_work(kind="IMAGE", request=None, execution_id=1) -> ExecutionWork:
    from lux_model_connector.protocol import InputGenerationPayload
    from lux_model_connector.store import ExecutionRecord, ExecutionWork

    payload = InputGenerationPayload.model_validate(
        {
            "identity": {
                "user_id": str(uuid4()),
                "session_id": str(uuid4()),
                "task_id": str(uuid4()),
                "operation_id": str(uuid4()),
            },
            "kind": kind,
            "request": request
            or (
                {"model_id": "arbitrary-upstream-endpoint", "prompt": "image"}
                if kind == "IMAGE"
                else {
                    "model_id": "arbitrary-video-endpoint",
                    "content": [{"type": "text", "text": "video"}],
                }
            ),
        }
    )
    identity = payload.identity
    execution = ExecutionRecord(
        execution_id,
        identity.user_id,
        identity.session_id,
        identity.task_id,
        identity.operation_id,
        kind,
        payload,
        "ACCEPTED",
        None,
        None,
        2,
    )
    return ExecutionWork(execution, None, None, None, None)


def image_receipt(partial=False) -> SeedreamReceipt:
    from lux_model_connector.byteplus import SeedreamReceipt

    data = [
        {"url": "https://byteplus.example/image.png", "output_format": "png"},
        {"b64_json": "aW1hZ2U=", "output_format": "jpeg"},
    ]
    if partial:
        data.append({"error": {"code": "ContentBlocked", "message": "blocked"}})
    return SeedreamReceipt.model_validate(
        {"created": 1, "model": "provider-resolved-model", "data": data}
    )


def video_receipt(
    status="succeeded", external_id="provider-video-1"
) -> SeedanceReceipt:
    from lux_model_connector.byteplus import SeedanceReceipt

    result = {"id": external_id, "model": "video-model", "status": status}
    if status == "succeeded":
        result["content"] = {
            "video_url": "https://byteplus.example/video.mp4",
            "last_frame_url": "https://byteplus.example/last.png",
        }
    if status == "failed":
        result["error"] = {"code": "ContentBlocked", "message": "blocked"}
    return SeedanceReceipt.model_validate(result)


class StoreDouble:
    def __init__(self, *work: ExecutionWork) -> None:
        self.work = {item.execution.id: item for item in work}
        self.claimed: set[int] = set()
        self.transitions: list[tuple[int, str, GenerationOutcome | None]] = []
        self.prepared: list[ProviderSubmissionPayload] = []
        self.receipts: list[SeedreamReceipt | SeedanceReceipt | None] = []
        self.saved: dict[int, list[StoredResult]] = {}
        self.receipt_failures = 0
        self.allow_begin = True
        self.allow_prepare = True
        self.allow_result_claim = True
        self.allow_completion = True
        self.scan_cursors: list[int] = []
        self.thread_ids: list[int] = []
        self.renewal_results: list[bool] = []
        self.renewed: list[ExecutionClaim] = []

    def _called(self) -> None:
        self.thread_ids.append(threading.get_ident())

    def _find(self, user_id, task_id) -> ExecutionWork:
        return next(
            item
            for item in self.work.values()
            if item.execution.user_id == user_id and item.execution.task_id == task_id
        )

    def claim_execution(
        self, *, account_ref=None, normalized_routes=()
    ) -> ExecutionClaim | None:
        from lux_model_connector.store import ExecutionClaim

        self._called()
        for item in self.work.values():
            if (
                item.execution.status in ("ACCEPTED", "RUNNING")
                and item.submission_started_at is None
                and item.provider_receipt is None
                and item.execution.id not in self.claimed
                and (
                    item.submission_payload is None
                    or account_ref is None
                    or item.submission_payload.account_ref == account_ref
                    or any(
                        item.submission_payload.account_ref == route.account_ref
                        and getattr(item.submission_payload, "binding", None)
                        == route.binding
                        and item.submission_payload.request.model_selection
                        == route.model_selection
                        and item.execution.kind == route.media_type
                        for route in normalized_routes
                    )
                )
            ):
                self.claimed.add(item.execution.id)
                return ExecutionClaim(item, str(uuid4()))
        return None

    def prepare_submission(self, claim, payload) -> bool:
        self._called()
        if not self.allow_prepare:
            return False
        item = self.work[claim.work.execution.id]
        execution = replace(item.execution, status="RUNNING")
        self.work[execution.id] = replace(
            item, execution=execution, submission_payload=payload
        )
        self.transitions.append((execution.id, "RUNNING", None))
        self.prepared.append(payload)
        return True

    def begin_submission(self, claim) -> bool:
        self._called()
        item = self.work[claim.work.execution.id]
        if not self.allow_begin or item.submission_started_at is not None:
            return False
        self.work[item.execution.id] = replace(
            item, submission_started_at=datetime.now(UTC)
        )
        return True

    def save_provider_receipt(self, user_id, task_id, external_id, receipt) -> bool:
        self._called()
        if self.receipt_failures:
            self.receipt_failures -= 1
            raise OSError("database temporarily unavailable")
        item = self._find(user_id, task_id)
        if item.execution.status != "RUNNING":
            return False
        self.work[item.execution.id] = replace(
            item, provider_external_id=external_id, provider_receipt=receipt
        )
        self.receipts.append(receipt)
        return True

    def release_execution(self, claim) -> bool:
        self._called()
        self.claimed.discard(claim.work.execution.id)
        return True

    def renew_execution(self, claim) -> bool:
        self._called()
        self.renewed.append(claim)
        return self.renewal_results.pop(0) if self.renewal_results else True

    def save_submission_failure(self, user_id, task_id, error) -> bool:
        from lux_model_connector.protocol import GenerationFailureOutcome
        from lux_model_connector.store import ExecutionClaim

        self._called()
        if self.receipt_failures:
            self.receipt_failures -= 1
            raise OSError("database temporarily unavailable")
        item = self._find(user_id, task_id)
        return self.complete_execution(
            ExecutionClaim(item, "late-original-submit"),
            GenerationFailureOutcome(status="FAILED", error=error),
        )

    def fail_execution(self, claim, error: GenerationError) -> bool:
        from lux_model_connector.protocol import GenerationFailureOutcome

        self._called()
        return self.complete_execution(
            claim, GenerationFailureOutcome(status="FAILED", error=error)
        )

    def scan_provider_work(self, after_id=0, limit=32) -> tuple[ExecutionWork, ...]:
        self._called()
        self.scan_cursors.append(after_id)
        return tuple(
            item
            for _, item in sorted(self.work.items())
            if item.execution.id > after_id
            and item.execution.status == "RUNNING"
            and item.submission_payload is not None
            and (item.provider_external_id or item.provider_receipt is not None)
        )[:limit]

    def claim_result(self, work) -> ExecutionClaim | None:
        from lux_model_connector.store import ExecutionClaim

        self._called()
        item = self.work[work.execution.id]
        if (
            not self.allow_result_claim
            or item.execution.id in self.claimed
            or item.execution.status != "RUNNING"
            or item.provider_receipt is None
        ):
            return None
        self.claimed.add(item.execution.id)
        return ExecutionClaim(item, str(uuid4()))

    def saved_results(self, user_id, task_id) -> tuple[StoredResult, ...]:
        self._called()
        return tuple(self.saved.get(self._find(user_id, task_id).execution.id, []))

    def save_result(self, claim, result) -> bool:
        self._called()
        if not self.allow_completion:
            return False
        self.saved.setdefault(claim.work.execution.id, []).append(result)
        return True

    def result_sources(self, claim):
        from lux_model_connector.protocol import ResultSource

        self._called()
        return tuple(
            ResultSource(
                url=f"https://connector.example/results/{result.source_index}",
                filename=result.filename,
                media_type=result.media_type,
            )
            for result in self.saved.get(claim.work.execution.id, [])
        )

    def complete_execution(self, claim, outcome) -> bool:
        self._called()
        if not self.allow_completion:
            return False
        item = self.work[claim.work.execution.id]
        self.work[item.execution.id] = replace(
            item,
            execution=replace(item.execution, status=outcome.status, outcome=outcome),
        )
        self.transitions.append((item.execution.id, outcome.status, outcome))
        return True


class ClientDouble:
    def __init__(self) -> None:
        from lux_model_connector.byteplus import BytePlusSettings

        self.settings = BytePlusSettings(api_key="fake-offline-key")
        self.image_calls = []
        self.video_calls = []
        self.query_calls: list[str] = []
        self.image_response: SeedreamReceipt | Exception = image_receipt()
        self.video_response: SeedanceReceipt | Exception = video_receipt()

    async def generate_image(self, request):
        self.image_calls.append(request)
        if isinstance(self.image_response, Exception):
            raise self.image_response
        return self.image_response

    async def create_video(self, request):
        from lux_model_connector.byteplus import VideoCreated

        self.video_calls.append(request)
        return VideoCreated(id="provider-video-1")

    async def get_video(self, external_id):
        self.query_calls.append(external_id)
        if isinstance(self.video_response, Exception):
            raise self.video_response
        return self.video_response


class ResultsDouble:
    def __init__(self) -> None:
        self.outputs: list[PreparedOutput] = []
        self.verified: list[StoredResult] = []
        self.errors: list[Exception | None] = []
        self.read_error: Exception | None = None

    async def preserve(self, user_id, task_id, output) -> StoredResult:
        from lux_model_connector.results import StoredResult

        self.outputs.append(output)
        if self.errors:
            error = self.errors.pop(0)
            if error is not None:
                raise error
        extension = "mp4" if output.media_kind == "VIDEO" else "png"
        return StoredResult(
            source_index=output.source_index,
            store_ref="b" * 64,
            bucket="test-bucket",
            object_key=f"{user_id}/{task_id}/{output.source_index}",
            filename=f"{output.source_index}.{extension}",
            media_type=f"video/{extension}" if extension == "mp4" else "image/png",
            sha256="a" * 64,
            byte_length=10,
        )

    def read(self, stored):
        from lux_model_connector.results import S3Object

        if self.read_error is not None:
            raise self.read_error
        self.verified.append(stored)
        return S3Object(
            io.BytesIO(b"test image"), stored.media_type, stored.byte_length
        )


def worker_parts(kind="IMAGE", request=None):
    from lux_model_connector.execution import ProviderWorker

    work = initial_work(kind, request)
    store, client, results = StoreDouble(work), ClientDouble(), ResultsDouble()
    return ProviderWorker(store, client, results), store, client, results


def test_image_preserves_complete_ordered_manifest_before_success():
    async def scenario():
        worker, store, client, results = worker_parts()
        await worker.step()
        assert client.image_calls[0].model_id == "arbitrary-upstream-endpoint"
        assert [entry.source_index for entry in results.outputs] == [0, 1]
        assert results.outputs[0].url == "https://byteplus.example/image.png"
        assert results.outputs[1].b64_json == "aW1hZ2U="
        assert [entry[1] for entry in store.transitions] == ["RUNNING", "SUCCEEDED"]
        outcome = store.transitions[-1][2]
        assert [source.url for source in outcome.sources] == [
            "https://connector.example/results/0",
            "https://connector.example/results/1",
        ]
        assert store.receipts == [client.image_response]

    asyncio.run(scenario())


def test_video_creation_is_checkpointed_then_original_id_polled():
    async def scenario():
        worker, store, client, results = worker_parts("VIDEO")
        await worker.step()
        assert len(client.video_calls) == 1
        assert client.video_calls[0].model_id == "arbitrary-video-endpoint"
        assert client.query_calls == ["provider-video-1"]
        assert [entry.media_kind for entry in results.outputs] == ["VIDEO", "IMAGE"]
        assert [entry[1] for entry in store.transitions] == ["RUNNING", "SUCCEEDED"]
        await worker.step()
        assert len(client.video_calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["queued", "running"])
def test_nonterminal_video_observations_do_not_emit_repeated_running_events(status):
    async def scenario():
        worker, store, client, results = worker_parts("VIDEO")
        client.video_response = video_receipt(status)
        await worker.step()
        await worker.step()
        assert len(client.video_calls) == 1
        assert client.query_calls == ["provider-video-1", "provider-video-1"]
        assert [entry[1] for entry in store.transitions] == ["RUNNING"]
        assert results.outputs == []

    asyncio.run(scenario())


def test_invalid_input_fails_before_submission_marker_and_http():
    async def scenario():
        worker, store, client, results = worker_parts(request={"model_id": "   "})
        await worker.step()
        assert client.image_calls == []
        assert store.prepared == []
        assert store.transitions[-1][2].error.stage == "VALIDATION"
        assert next(iter(store.work.values())).submission_started_at is None
        assert results.outputs == []

    asyncio.run(scenario())


@pytest.mark.parametrize("failed_guard", ["allow_prepare", "allow_begin"])
def test_lost_submission_claim_cannot_call_provider(failed_guard):
    async def scenario():
        worker, store, client, _ = worker_parts()
        setattr(store, failed_guard, False)
        await worker.step()
        assert client.image_calls == []

    asyncio.run(scenario())


def test_partial_image_error_retains_receipt_and_fails_without_partial_success():
    async def scenario():
        worker, store, client, results = worker_parts()
        client.image_response = image_receipt(partial=True)
        await worker.step()
        assert store.receipts == [client.image_response]
        assert store.transitions[-1][1] == "FAILED"
        assert store.transitions[-1][2].error.stage == "PROVIDER"
        assert results.outputs == []

    asyncio.run(scenario())


def test_receipt_checkpoint_failure_retries_same_receipt_without_new_post():
    async def scenario():
        worker, store, client, _ = worker_parts()
        store.receipt_failures = 1
        await worker.step()
        assert len(client.image_calls) == 1
        assert store.transitions[-1][1] == "RUNNING"
        await worker.step()
        assert len(client.image_calls) == 1
        assert store.receipts == [client.image_response]
        assert store.transitions[-1][1] == "SUCCEEDED"

    asyncio.run(scenario())


def test_store_calls_never_block_event_loop_thread():
    async def scenario():
        worker, store, _, _ = worker_parts()
        loop_thread = threading.get_ident()
        await worker.step()
        assert store.thread_ids
        assert all(thread != loop_thread for thread in store.thread_ids)

    asyncio.run(scenario())


def running_work(
    kind="IMAGE", execution_id=1, receipt=None, account_ref="byteplus-primary"
):
    from lux_model_connector.byteplus import (
        ProviderSubmissionPayload,
        SeedanceRequest,
        SeedreamRequest,
    )

    work = initial_work(kind, execution_id=execution_id)
    request_type = SeedreamRequest if kind == "IMAGE" else SeedanceRequest
    submission = ProviderSubmissionPayload(
        kind=kind,
        account_ref=account_ref,
        request=request_type.model_validate(
            work.execution.input_generation_payload.request.root
        ),
    )
    return replace(
        work,
        execution=replace(work.execution, status="RUNNING"),
        submission_payload=submission,
        submission_started_at=datetime.now(UTC),
        provider_external_id=f"video-{execution_id}" if kind == "VIDEO" else None,
        provider_receipt=receipt,
    )


def test_uncertain_submission_remains_running_after_worker_restart_without_resubmit():
    async def scenario():
        from lux_model_connector.byteplus import BytePlusUncertainSubmission
        from lux_model_connector.execution import ProviderWorker

        worker, store, client, results = worker_parts()
        client.image_response = BytePlusUncertainSubmission("acceptance unknown")
        await worker.step()
        await worker.step()
        await ProviderWorker(store, client, results).step()
        assert len(client.image_calls) == 1
        assert [entry[1] for entry in store.transitions] == ["RUNNING"]
        assert results.outputs == []
        assert next(iter(store.work.values())).submission_started_at is not None

    asyncio.run(scenario())


def test_definitive_rejection_retries_saved_failure_after_db_error_without_new_post():
    async def scenario():
        from lux_model_connector.byteplus import BytePlusRejected, ProviderAPIError

        worker, store, client, results = worker_parts()
        client.image_response = BytePlusRejected(
            ProviderAPIError(code="InvalidParameter", message="input rejected"), 400
        )
        store.receipt_failures = 1
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        await worker.step()
        assert len(client.image_calls) == 1
        error = store.transitions[-1][2].error
        assert error.stage == "PROVIDER"
        assert error.provider_diagnostic.provider_code == "InvalidParameter"
        assert store.transitions[-1][1] == "FAILED"
        assert results.outputs == []

    asyncio.run(scenario())


def test_persisted_image_receipt_recovers_without_another_generation_call():
    async def scenario():
        from lux_model_connector.execution import ProviderWorker

        store = StoreDouble(running_work(receipt=image_receipt()))
        client, results = ClientDouble(), ResultsDouble()
        await ProviderWorker(store, client, results).step()
        assert client.image_calls == []
        assert [entry[1] for entry in store.transitions] == ["SUCCEEDED"]
        assert [entry.source_index for entry in results.outputs] == [0, 1]

    asyncio.run(scenario())


def test_query_error_isolated_and_original_other_video_continues():
    async def scenario():
        from lux_model_connector.byteplus import BytePlusQueryError
        from lux_model_connector.execution import ProviderWorker

        class QueryClient(ClientDouble):
            async def get_video(self, external_id):
                self.query_calls.append(external_id)
                if external_id == "video-1":
                    raise BytePlusQueryError()
                return video_receipt(external_id=external_id)

        store = StoreDouble(running_work("VIDEO", 1), running_work("VIDEO", 2))
        client, results = QueryClient(), ResultsDouble()
        await ProviderWorker(store, client, results).step()
        assert client.query_calls == ["video-1", "video-2"]
        assert store.work[1].execution.status == "RUNNING"
        assert store.work[2].execution.status == "SUCCEEDED"
        assert client.video_calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["failed", "cancelled", "expired"])
def test_terminal_video_failure_preserves_evidence_and_never_downloads(status):
    async def scenario():
        worker, store, client, results = worker_parts("VIDEO")
        client.video_response = video_receipt(status)
        await worker.step()
        assert isinstance(store.receipts[-1], type(client.video_response))
        assert store.transitions[-1][1] == "FAILED"
        assert store.transitions[-1][2].error.stage == "PROVIDER"
        assert results.outputs == []
        await worker.step()
        assert len(client.video_calls) == 1
        assert len(client.query_calls) == 1

    asyncio.run(scenario())


def test_recoverable_result_error_reuses_saved_object_and_downloads_remaining_only():
    async def scenario():
        from lux_model_connector.results import ResultPreservationError

        worker, store, client, results = worker_parts()
        results.errors = [None, ResultPreservationError("put acknowledgment unknown")]
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        assert len(store.saved[1]) == 1
        await worker.step()
        assert store.transitions[-1][1] == "SUCCEEDED"
        assert len(client.image_calls) == 1
        assert [output.source_index for output in results.outputs] == [0, 1, 1]
        assert [saved.source_index for saved in results.verified] == [0]

    asyncio.run(scenario())


def test_unavailable_original_keeps_preservation_running_without_resubmitting():
    async def scenario():
        from lux_model_connector.results import ResultSourceUnavailable

        worker, store, client, results = worker_parts()
        results.errors = [ResultSourceUnavailable("gone")]
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        assert store.transitions[-1][2] is None
        assert store.saved.get(1, []) == []
        results.errors = [ResultSourceUnavailable("still gone")]
        await worker.step()
        assert len(client.image_calls) == 1
        assert store.transitions[-1][1] == "RUNNING"

    asyncio.run(scenario())


def test_saved_object_integrity_error_prevents_success():
    async def scenario():
        from lux_model_connector.results import ResultPreservationError

        worker, store, client, results = worker_parts()
        results.errors = [None, ResultPreservationError("temporary transfer error")]
        await worker.step()
        results.read_error = ResultPreservationError("retained metadata changed")
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        assert len(client.image_calls) == 1
        assert len(store.saved[1]) == 1
        assert len(results.outputs) == 2

    asyncio.run(scenario())


def test_expired_result_claim_cannot_commit_success():
    async def scenario():
        worker, store, client, results = worker_parts()
        store.allow_completion = False
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        assert len(results.outputs) == 1
        assert len(client.image_calls) == 1

    asyncio.run(scenario())


def test_independent_bounded_pages_advance_past_unfinished_low_id_then_wrap():
    async def scenario():
        from lux_model_connector.execution import ProviderWorker

        class RunningClient(ClientDouble):
            async def get_video(self, external_id):
                self.query_calls.append(external_id)
                return video_receipt("running", external_id)

        store = StoreDouble(*(running_work("VIDEO", index) for index in range(1, 6)))
        client, results = RunningClient(), ResultsDouble()
        worker = ProviderWorker(store, client, results, page_size=2)
        for _ in range(4):
            await worker.step()
        assert client.query_calls == [
            "video-1",
            "video-2",
            "video-3",
            "video-4",
            "video-5",
            "video-1",
            "video-2",
        ]
        assert store.scan_cursors == [0, 2, 4, 0]
        assert results.outputs == []

    asyncio.run(scenario())


def test_different_provider_account_never_queries_or_resubmits_original():
    async def scenario():
        from lux_model_connector.execution import ProviderWorker

        original = running_work("VIDEO", account_ref="a-different-account")
        store, client, results = StoreDouble(original), ClientDouble(), ResultsDouble()
        await ProviderWorker(store, client, results).step()
        assert client.video_calls == []
        assert client.query_calls == []
        assert store.transitions == []

    asyncio.run(scenario())


def test_incomplete_success_observation_can_be_requeried_without_freezing_receipt():
    async def scenario():
        from lux_model_connector.byteplus import SeedanceReceipt

        worker, store, client, results = worker_parts("VIDEO")
        client.video_response = SeedanceReceipt(
            id="provider-video-1", status="succeeded"
        )
        await worker.step()
        assert store.receipts == [None]
        assert store.transitions[-1][1] == "RUNNING"
        client.video_response = video_receipt()
        await worker.step()
        assert store.transitions[-1][1] == "SUCCEEDED"
        assert len(client.query_calls) == 2
        assert len(client.video_calls) == 1
        assert len(results.outputs) == 2

    asyncio.run(scenario())


def test_cancelled_post_never_resubmits_when_same_worker_cycle_is_waiting():
    async def scenario():
        from lux_model_connector.execution import ProviderWorker

        entered = asyncio.Event()

        class SlowClient(ClientDouble):
            async def generate_image(self, request):
                self.image_calls.append(request)
                entered.set()
                await asyncio.Event().wait()

        store = StoreDouble(initial_work())
        client, results = SlowClient(), ResultsDouble()
        worker = ProviderWorker(store, client, results)
        first = asyncio.create_task(worker.step())
        await entered.wait()
        second = asyncio.create_task(worker.step())
        await asyncio.sleep(0)
        assert len(client.image_calls) == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await second
        assert len(client.image_calls) == 1
        assert store.transitions[-1][1] == "RUNNING"
        assert store.claimed == set()

    asyncio.run(scenario())


def test_run_cancellation_does_not_own_http_client_shutdown():
    async def scenario():
        from lux_model_connector.execution import ProviderWorker

        store, client, results = StoreDouble(), ClientDouble(), ResultsDouble()
        worker = ProviderWorker(store, client, results, interval_seconds=0.01)
        task = asyncio.create_task(worker.run())
        async with asyncio.timeout(1):
            while not store.scan_cursors:
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


@pytest.mark.parametrize("renewals,retained", [([False], 0), ([True, False], 1)])
def test_result_claim_must_be_renewed_before_each_original_output(renewals, retained):
    async def scenario():
        worker, store, client, results = worker_parts()
        store.renewal_results = renewals.copy()
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        assert len(results.outputs) == retained
        assert len(store.saved.get(1, [])) == retained
        assert len(client.image_calls) == 1

    asyncio.run(scenario())


def test_lost_claim_before_retained_object_verification_stops_recovery():
    async def scenario():
        from lux_model_connector.results import ResultPreservationError

        worker, store, client, results = worker_parts()
        results.errors = [None, ResultPreservationError("transfer deferred")]
        await worker.step()
        store.renewal_results = [False]
        await worker.step()
        assert results.verified == []
        assert store.transitions[-1][1] == "RUNNING"
        assert len(client.image_calls) == 1

    asyncio.run(scenario())


def test_late_retained_object_can_complete_after_original_url_disappears():
    async def scenario():
        from lux_model_connector.results import ResultSourceUnavailable

        worker, store, client, results = worker_parts()
        results.errors = [ResultSourceUnavailable("original URL gone")]
        await worker.step()
        assert store.transitions[-1][1] == "RUNNING"
        # The result boundary can recover an earlier in-flight PUT by its stable
        # output slot, even though the original provider URL is still unavailable.
        await worker.step()
        assert store.transitions[-1][1] == "SUCCEEDED"
        assert len(client.image_calls) == 1
        assert store.receipts == [client.image_response]

    asyncio.run(scenario())


def test_missing_requested_last_frame_is_requeried_before_terminal_receipt():
    async def scenario():
        from lux_model_connector.byteplus import SeedanceReceipt

        request = {
            "model_id": "upstream-video-model",
            "content": [{"type": "text", "text": "video"}],
            "return_last_frame": True,
        }
        worker, store, client, results = worker_parts("VIDEO", request)
        client.video_response = SeedanceReceipt.model_validate(
            {
                "id": "provider-video-1",
                "status": "succeeded",
                "content": {"video_url": "https://byteplus.example/video.mp4"},
            }
        )
        await worker.step()
        assert store.receipts == [None]
        assert store.transitions[-1][1] == "RUNNING"
        assert results.outputs == []
        client.video_response = video_receipt()
        await worker.step()
        assert store.transitions[-1][1] == "SUCCEEDED"
        assert len(client.video_calls) == 1
        assert len(client.query_calls) == 2
        assert len(results.outputs) == 2

    asyncio.run(scenario())


def test_late_video_observation_uses_first_persisted_receipt_and_clears_pending():
    async def scenario():
        from lux_model_connector.byteplus import SeedanceReceipt
        from lux_model_connector.execution import ProviderWorker

        original = running_work("VIDEO", receipt=video_receipt(external_id="video-1"))

        class ConcurrentObservationStore(StoreDouble):
            def scan_provider_work(self, after_id=0, limit=32):
                page = super().scan_provider_work(after_id, limit)
                return tuple(
                    replace(work, provider_receipt=None)
                    if work.execution.id == 1
                    else work
                    for work in page
                )

            def save_provider_receipt(self, user_id, task_id, external_id, receipt):
                item = self._find(user_id, task_id)
                if (
                    item.provider_receipt is not None
                    and receipt != item.provider_receipt
                ):
                    return False
                return super().save_provider_receipt(
                    user_id, task_id, external_id, receipt
                )

        store, client, results = (
            ConcurrentObservationStore(original),
            ClientDouble(),
            ResultsDouble(),
        )
        client.video_response = SeedanceReceipt.model_validate(
            {
                "id": "video-1",
                "status": "succeeded",
                "updated_at": 2,
                "content": {"video_url": "https://byteplus.example/late.mp4"},
            }
        )
        worker = ProviderWorker(store, client, results)
        await worker.step()
        assert store.work[1].provider_receipt == original.provider_receipt
        assert results.outputs[0].url == "https://byteplus.example/video.mp4"
        assert store.work[1].execution.status == "SUCCEEDED"
        store.work[2] = initial_work(execution_id=2)
        await worker.step()
        assert len(client.image_calls) == 1
        assert store.work[2].execution.status == "SUCCEEDED"

    asyncio.run(scenario())
