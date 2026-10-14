"""MySQL proof of dispatch exclusion, receipt recovery, and preserved results."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_store import TASK, USER, submit

from lux_model_connector.byteplus import (
    ProviderSubmissionPayload,
    SeedanceReceipt,
    SeedanceRequest,
    SeedreamReceipt,
    SeedreamRequest,
)
from lux_model_connector.protocol import (
    GenerationError,
    GenerationFailureOutcome,
    GenerationSuccessOutcome,
)
from lux_model_connector.results import StoredResult
from lux_model_connector.store import ConnectorStore, StoreConflict


def payload(kind="IMAGE"):
    request = (
        SeedreamRequest(model_id="any-new-id", prompt="cat")
        if kind == "IMAGE"
        else SeedanceRequest(
            model_id="arbitrary-video-model", content=[{"type": "text", "text": "cat"}]
        )
    )
    return ProviderSubmissionPayload(
        provider_id="byteplus",
        account_ref="byteplus-primary",
        kind=kind,
        request=request,
    )


def admit(store, kind="IMAGE"):
    body = payload(kind).request.model_dump(mode="json", exclude_unset=True)
    store.receive(submit(kind=kind, request=body))
    claim = store.claim_execution()
    assert store.prepare_submission(claim, payload(kind))
    return claim


def receipt():
    return SeedreamReceipt(
        created=1, model="any-new-id", data=[{"url": "https://example.org/a.png"}]
    )


def expire(store):
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE generation_execution SET execution_lease_until="
            "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND"
        )
        connection.commit()


def test_one_claim_and_durable_marker_prevent_resubmission(store):
    store.receive(
        submit(request=payload().request.model_dump(mode="json", exclude_unset=True))
    )
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: store.claim_execution(), range(4)))
    claim = next(c for c in claims if c)
    assert sum(c is not None for c in claims) == 1
    assert store.prepare_submission(claim, payload())
    assert store.begin_submission(claim)
    assert not store.begin_submission(claim)
    expire(store)
    assert store.claim_execution() is None
    assert store.execution(USER, TASK).status == "RUNNING"
    assert store.execution(USER, TASK).next_event_sequence == 3


def test_expired_prepare_reclaimed_before_first_dispatch(store):
    claim = admit(store)
    expire(store)
    replacement = store.claim_execution()
    assert replacement.work.submission_payload == payload()
    assert not store.begin_submission(claim)
    assert store.prepare_submission(replacement, payload())
    assert store.begin_submission(replacement)


def test_original_receipt_survives_expired_claim_and_is_immutable(store):
    claim = admit(store)
    assert store.begin_submission(claim)
    expire(store)
    assert store.save_provider_receipt(USER, TASK, None, receipt())
    assert store.save_provider_receipt(USER, TASK, None, receipt())
    work = store.scan_provider_work()[0]
    assert work.provider_receipt == receipt()
    assert store.claim_execution() is None
    changed = SeedreamReceipt(
        created=1,
        model="any-new-id",
        data=[{"url": "https://example.org/different.png"}],
    )
    with pytest.raises(StoreConflict):
        store.save_provider_receipt(USER, TASK, None, changed)


def test_video_id_checkpoint_and_query_observations(store):
    claim = admit(store, "VIDEO")
    assert store.begin_submission(claim)
    assert store.save_provider_receipt(USER, TASK, "cgt-original", None)
    work = store.scan_provider_work()[0]
    assert work.provider_external_id == "cgt-original"
    assert store.claim_result(work) is None
    done = SeedanceReceipt(
        id="cgt-original",
        status="succeeded",
        content={"video_url": "https://example.org/a.mp4"},
    )
    assert store.save_provider_receipt(USER, TASK, "cgt-original", done)
    assert store.claim_result(store.scan_provider_work()[0])
    with pytest.raises(StoreConflict):
        store.save_provider_receipt(USER, TASK, "cgt-other", None)


def test_result_manifest_complete_claim_and_late_writer(store):
    claim = admit(store)
    assert store.begin_submission(claim)
    assert store.save_provider_receipt(USER, TASK, None, receipt())
    work = store.scan_provider_work()[0]
    store.release_execution(claim)
    result_claim = store.claim_result(work)
    result = StoredResult(
        source_index=0,
        store_ref="a" * 64,
        bucket="test-bucket",
        object_key="result/key",
        filename="image.png",
        media_type="image/png",
        sha256="0" * 64,
        byte_length=12,
    )
    assert store.save_result(result_claim, result)
    assert store.save_result(result_claim, result)
    sources = store.result_sources(result_claim)
    assert sources[0].url.startswith(
        "http://connector.test/internal/generation-results/"
    )
    token = sources[0].url.rsplit("/", 1)[-1]
    assert store.download_result(token) is None
    assert store.download_result("wrong") is None
    expire(store)
    replacement = store.claim_result(work)
    assert not store.complete_execution(
        result_claim, GenerationSuccessOutcome(status="SUCCEEDED", sources=sources)
    )
    assert store.complete_execution(
        replacement, GenerationSuccessOutcome(status="SUCCEEDED", sources=sources)
    )
    assert store.saved_results(USER, TASK) == (result,)
    assert store.execution(USER, TASK).status == "SUCCEEDED"
    assert store.download_result(token) == result


def test_validation_and_post_rejection_are_distinct_claim_paths(store):
    store.receive(submit())
    claim = store.claim_execution()
    error = GenerationError(
        code="INVALID_REQUEST", stage="VALIDATION", message="unknown field"
    )
    assert store.fail_execution(claim, error)
    assert store.execution(USER, TASK).outcome == GenerationFailureOutcome(
        status="FAILED", error=error
    )


def test_post_failure_saved_after_lease_expiry(store):
    claim = admit(store)
    assert store.begin_submission(claim)
    expire(store)
    error = GenerationError(
        code="PROVIDER_REJECTED", stage="PROVIDER", message="invalid parameter"
    )
    assert store.save_submission_failure(USER, TASK, error)
    assert store.save_submission_failure(USER, TASK, error)
    assert store.execution(USER, TASK).status == "FAILED"


def test_v1_migration_keeps_accepted_execution(mysql_settings):
    # The v1 package snapshot is the schema actually used before providers.
    old = Path(__file__).parents[1] / "src/lux_model_connector/schema_v1.sql"
    store = ConnectorStore(mysql_settings, result_base_url="http://connector.test")
    with store.connection() as connection, connection.cursor() as cursor:
        for sql in old.read_text().split(";"):
            if sql.strip():
                cursor.execute(sql)
        cursor.execute(
            "INSERT INTO connector_schema_version VALUES (1,'lux-model-connector',1)"
        )
        connection.commit()
    store.receive(submit())
    original = store.execution(USER, TASK)
    store.initialize()
    assert store.execution(USER, TASK) == original
    assert store.claim_execution()
    store.initialize()


def test_scan_advances_and_unknown_dispatch_excluded(store):
    claim = admit(store)
    store.begin_submission(claim)
    assert store.scan_provider_work() == ()
    store.save_provider_receipt(USER, TASK, None, receipt())
    work = store.scan_provider_work()[0]
    assert store.scan_provider_work(after_id=work.execution.id) == ()


def test_late_video_observation_cannot_replace_original_or_lease(store):
    claim = admit(store, "VIDEO")
    store.begin_submission(claim)
    store.save_provider_receipt(USER, TASK, "cgt-original", None)
    original = SeedanceReceipt(
        id="cgt-original",
        status="succeeded",
        content={"video_url": "https://example.org/a.mp4"},
        updated_at=1,
    )
    store.save_provider_receipt(USER, TASK, "cgt-original", original)
    work = store.scan_provider_work()[0]
    result_claim = store.claim_result(work)
    late = original.model_copy(update={"updated_at": 2})
    assert not store.save_provider_receipt(USER, TASK, "cgt-original", late)
    assert store.scan_provider_work()[0].provider_receipt == original
    assert store.renew_execution(result_claim)
    expire(store)
    assert not store.renew_execution(result_claim)


def test_success_requires_complete_original_receipt_manifest(store):
    claim = admit(store)
    store.begin_submission(claim)
    images = SeedreamReceipt(
        data=[
            {"url": "https://example.org/1.png"},
            {"url": "https://example.org/2.png"},
        ]
    )
    store.save_provider_receipt(USER, TASK, None, images)
    result_claim = store.claim_result(store.scan_provider_work()[0])
    one = StoredResult(
        source_index=0,
        store_ref="a" * 64,
        bucket="bucket",
        object_key="key",
        filename="0.png",
        media_type="image/png",
        sha256="0" * 64,
        byte_length=12,
    )
    store.save_result(result_claim, one)
    with pytest.raises(StoreConflict):
        store.complete_execution(
            result_claim,
            GenerationSuccessOutcome(
                status="SUCCEEDED", sources=store.result_sources(result_claim)
            ),
        )
    assert store.execution(USER, TASK).status == "RUNNING"


def test_missing_requested_last_frame_receipt_not_frozen(store):
    request = SeedanceRequest(
        model_id="new-video-id",
        content=[{"type": "text", "text": "cat"}],
        return_last_frame=True,
    )
    selected = ProviderSubmissionPayload(
        account_ref="byteplus-primary", kind="VIDEO", request=request
    )
    store.receive(submit(kind="VIDEO", request=request.model_dump(mode="json")))
    claim = store.claim_execution()
    store.prepare_submission(claim, selected)
    store.begin_submission(claim)
    store.save_provider_receipt(USER, TASK, "cgt-original", None)
    incomplete = SeedanceReceipt(
        id="cgt-original",
        status="succeeded",
        content={"video_url": "https://example.org/a.mp4"},
    )
    assert not store.save_provider_receipt(USER, TASK, "cgt-original", incomplete)
    assert store.scan_provider_work()[0].provider_receipt is None


def test_partial_migration_restarts_without_losing_owned_records(mysql_settings):
    from importlib.resources import files

    store = ConnectorStore(mysql_settings)
    with store.connection() as connection, connection.cursor() as cursor:
        for sql in (
            files("lux_model_connector")
            .joinpath("schema_v1.sql")
            .read_text()
            .split(";")
        ):
            if sql.strip():
                cursor.execute(sql)
        cursor.execute(
            "INSERT INTO connector_schema_version VALUES (1,'lux-model-connector',1)"
        )
        cursor.execute(
            "ALTER TABLE generation_execution ADD COLUMN execution_lease_token "
            "CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL"
        )
        connection.commit()
    store.receive(submit())
    store.initialize()
    assert store.execution(USER, TASK).status == "ACCEPTED"
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT version FROM connector_schema_version")
        assert cursor.fetchone()[0] == 3


def test_enabled_provider_capacity_is_checked_before_dispatch(store):
    store.validate_provider_capacity(128 * 1024 * 1024)
    with pytest.raises(StoreConflict):
        store.validate_provider_capacity(1024 * 1024 * 1024)


@pytest.mark.parametrize(
    "base", ["https://connector.test/?token=secret", "https://connector.test/#fragment"]
)
def test_result_gateway_base_rejects_query_and_fragment(base):
    from lux_model_connector.store import MySQLSettings

    settings = MySQLSettings(
        host="localhost", port=3306, user="connector", password="", database="connector"
    )
    with pytest.raises(ValueError):
        ConnectorStore(settings, result_base_url=base)
