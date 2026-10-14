from concurrent.futures import ThreadPoolExecutor

import pytest
from test_provider_store import admit, receipt
from test_store import TASK, USER

from lux_model_connector.store import ConnectorStore, StoreConflict


def attachment(store, kind="VIDEO"):
    from lux_model_connector.reconciliation import (
        OriginalProviderEvidence,
        original_view,
    )

    claim = admit(store, kind)
    assert store.begin_submission(claim)
    store.release_execution(claim)
    work = store.reconciliation_work(USER, TASK)
    view = original_view(work)
    return OriginalProviderEvidence(
        binding=view.binding,
        external_id="cgt-original" if kind == "VIDEO" else None,
        image_receipt=receipt() if kind == "IMAGE" else None,
        evidence_reference="ticket:original-receipt",
        reason="Original provider receipt recovered",
    )


def test_attachment_is_atomic_idempotent_and_restart_safe(store, mysql_settings):
    assert hasattr(store, "attach_provider_evidence"), "Audited attachment is missing"
    attached = attachment(store)
    with ThreadPoolExecutor(max_workers=4) as workers:
        views = list(
            workers.map(
                lambda _: store.attach_provider_evidence(
                    attached, "operator", ("byteplus-primary",)
                ),
                range(4),
            )
        )
    assert all(view.external_id == "cgt-original" for view in views)
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM provider_reconciliation_audit")
        assert cursor.fetchone()[0] == 1
    restarted = ConnectorStore(mysql_settings)
    restarted.initialize()
    assert restarted.scan_provider_work()[0].provider_external_id == "cgt-original"
    assert restarted.claim_execution() is None
    assert restarted.execution(USER, TASK).status == "RUNNING"


@pytest.mark.parametrize("kind", ["IMAGE", "VIDEO"])
def test_conflicts_and_scopes_preserve_original(store, kind):
    attached = attachment(store, kind)
    for field, value in [
        ("account_ref", "other"),
        ("request_hash", "0" * 64),
        ("model_id", "other"),
        ("session_id", "00000000-0000-0000-0000-000000000099"),
    ]:
        with pytest.raises(StoreConflict):
            store.attach_provider_evidence(
                attached.model_copy(
                    update={
                        "binding": attached.binding.model_copy(update={field: value})
                    }
                ),
                "operator",
                ("byteplus-primary",),
            )
    with pytest.raises(StoreConflict):
        store.attach_provider_evidence(attached, "operator", ("other",))
    store.attach_provider_evidence(attached, "operator", ("byteplus-primary",))
    with pytest.raises(StoreConflict):
        store.attach_provider_evidence(
            attached.model_copy(update={"evidence_reference": "ticket:changed"}),
            "operator",
            ("byteplus-primary",),
        )
    assert store.claim_execution() is None


def test_disabled_routes_hide_invalid_attachment_before_validation():
    import asyncio

    import httpx

    from lux_model_connector.main import ServiceSettings, create_app

    async def scenario():
        app = create_app(ServiceSettings())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (
                await client.get("/internal/provider-reconciliation")
            ).status_code == 404
            assert (
                await client.post(
                    f"/internal/provider-reconciliation/{USER}/{TASK}/original-receipt",
                    json={},
                )
            ).status_code == 404

    asyncio.run(scenario())


def test_audit_insert_failure_rolls_back_provider_identity(store):
    import pymysql

    attached = attachment(store)
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TRIGGER reject_attachment BEFORE INSERT ON "
            "provider_reconciliation_audit FOR EACH ROW SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='audit unavailable'"
        )
        connection.commit()
    with pytest.raises(pymysql.MySQLError, match="audit unavailable"):
        store.attach_provider_evidence(attached, "operator", ("byteplus-primary",))
    work = store.reconciliation_work(USER, TASK)
    assert work.provider_external_id is None and work.provider_receipt is None
    assert store.claim_execution() is None


def test_v2_additive_upgrade_preserves_unresolved_original(store, mysql_settings):
    attached = attachment(store)
    original = store.reconciliation_work(USER, TASK)
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE provider_reconciliation_audit")
        cursor.execute("UPDATE connector_schema_version SET version=2")
        connection.commit()
    restarted = ConnectorStore(mysql_settings)
    restarted.initialize()
    assert restarted.reconciliation_work(USER, TASK) == original
    assert (
        restarted.attach_provider_evidence(
            attached, "operator", ("byteplus-primary",)
        ).external_id
        == "cgt-original"
    )


def test_operator_route_account_scope_and_trusted_actor(store, mysql_settings):
    import asyncio

    import httpx

    from lux_model_connector.main import ServiceSettings, create_app
    from lux_model_connector.reconciliation import ReconciliationSettings

    async def scenario():
        attached = attachment(store)
        app = create_app(
            ServiceSettings(
                mysql=mysql_settings,
                reconciliation=ReconciliationSettings(
                    True, "secret", "trusted-operator", ("byteplus-primary",)
                ),
            )
        )
        app.state.store = store
        path = f"/internal/provider-reconciliation/{USER}/{TASK}"
        headers = {"Authorization": "Bearer secret"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get(path)).status_code == 401
            assert (
                await client.post(path + "/original-receipt", json={})
            ).status_code == 401
            detail = await client.get(path, headers=headers)
            assert detail.status_code == 200 and "submission_payload" not in detail.text
            body = attached.model_dump(mode="json")
            body["operator_id"] = "untrusted"
            assert (
                await client.post(
                    path + "/original-receipt", headers=headers, json=body
                )
            ).status_code == 422
            body.pop("operator_id")
            assert (
                await client.post(
                    path + "/original-receipt", headers=headers, json=body
                )
            ).status_code == 200
        scoped = create_app(
            ServiceSettings(
                mysql=mysql_settings,
                reconciliation=ReconciliationSettings(
                    True, "secret", "trusted-operator", ("other",)
                ),
            )
        )
        scoped.state.store = store
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=scoped), base_url="http://test"
        ) as client:
            assert (await client.get(path, headers=headers)).status_code == 404
            assert (
                await client.get("/internal/provider-reconciliation", headers=headers)
            ).json() == {"executions": []}
            assert (
                await client.post(
                    path + "/original-receipt", headers=headers, json=body
                )
            ).status_code == 409
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT operator_id FROM provider_reconciliation_audit")
            assert cursor.fetchone()[0] == "trusted-operator"

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["IMAGE", "VIDEO"])
def test_attachment_recovers_original_query_or_preservation_without_submission(
    store, mysql_settings, kind
):
    import asyncio

    from test_execution import ClientDouble, ResultsDouble, video_receipt

    from lux_model_connector.execution import ProviderWorker

    attached = attachment(store, kind)
    store.attach_provider_evidence(attached, "operator", ("byteplus-primary",))
    restarted = ConnectorStore(mysql_settings, result_base_url="http://connector.test")
    restarted.initialize()
    client, results = ClientDouble(), ResultsDouble()
    client.video_response = video_receipt(external_id="cgt-original")
    asyncio.run(ProviderWorker(restarted, client, results).step())
    assert client.image_calls == [] and client.video_calls == []
    assert client.query_calls == (["cgt-original"] if kind == "VIDEO" else [])
    assert results.outputs and restarted.execution(USER, TASK).status == "SUCCEEDED"
    assert (
        restarted.attach_provider_evidence(
            attached, "operator", ("byteplus-primary",)
        ).status
        == "SUCCEEDED"
    )


def test_normalized_seedream_original_receipt_restores_adapter_owned_outputs(store):
    import asyncio

    from test_byteplus_adapters import image_plan
    from test_execution import ClientDouble, ResultsDouble
    from test_store import submit

    from lux_model_connector.byteplus import SeedreamReceipt
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import (
        NormalizedPreparedSubmission,
        decode_generation_request,
    )
    from lux_model_connector.reconciliation import (
        OriginalProviderEvidence,
        original_view,
    )

    async def scenario():
        plan = image_plan()
        request = decode_generation_request(plan, "IMAGE")
        store.receive(submit(request=plan))
        registry = byteplus_registry(ClientDouble(), ResultsDouble())
        registration, adapter = registry.resolve(request)
        prepared = NormalizedPreparedSubmission(
            account_ref=registration.account_ref,
            kind="IMAGE",
            binding=request.provider_binding,
            request=request,
            provider_payload=await adapter.translate(request, ()),
        )
        claim = store.claim_execution(normalized_routes=registry.registrations)
        assert store.prepare_submission(claim, prepared) and store.begin_submission(
            claim
        )
        store.release_execution(claim)
        view = original_view(store.reconciliation_work(USER, TASK))
        receipt = SeedreamReceipt(
            model=request.provider_binding.provider_model_id,
            data=[{"url": "https://example.org/original.png"}],
        )
        attached = OriginalProviderEvidence(
            binding=view.binding,
            image_receipt=receipt,
            evidence_reference="ticket:normalized",
            reason="Original Seedream response recovered",
        )
        store.attach_provider_evidence(
            attached, "operator", (registration.account_ref,)
        )
        saved = store.reconciliation_work(USER, TASK).provider_receipt
        assert saved.status == "SUCCEEDED"
        assert saved.original_image_receipt.model == receipt.model
        assert saved.outputs[0].url == receipt.data[0].url
        assert store.claim_execution(normalized_routes=registry.registrations) is None

    asyncio.run(scenario())


def test_existing_normalized_receipt_without_default_tag_remains_readable(store):
    # Older serialization omitted the default discriminator; its prepared archive
    # already establishes the receipt's schema, so reopening must preserve it.
    import asyncio

    from test_byteplus_adapters import image_plan
    from test_execution import ClientDouble, ResultsDouble
    from test_store import submit

    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import (
        NormalizedPreparedSubmission,
        NormalizedReceipt,
        decode_generation_request,
    )
    from lux_model_connector.protocol import canonical_json

    async def scenario():
        plan = image_plan()
        request = decode_generation_request(plan, "IMAGE")
        store.receive(submit(request=plan))
        registry = byteplus_registry(ClientDouble(), ResultsDouble())
        registration, adapter = registry.resolve(request)
        prepared = NormalizedPreparedSubmission(
            account_ref=registration.account_ref,
            kind="IMAGE",
            binding=request.provider_binding,
            request=request,
            provider_payload=await adapter.translate(request, ()),
        )
        claim = store.claim_execution(normalized_routes=registry.registrations)
        store.prepare_submission(claim, prepared)
        store.begin_submission(claim)
        original = NormalizedReceipt(status="FAILED")
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE generation_execution SET provider_receipt=%s",
                (canonical_json(original.model_dump(mode="json", exclude_unset=True)),),
            )
            connection.commit()
        assert store.reconciliation_work(USER, TASK).provider_receipt == original

    asyncio.run(scenario())


def test_terminal_or_already_observed_evidence_cannot_be_replaced(store):
    from lux_model_connector.protocol import GenerationError

    attached = attachment(store)
    store.save_submission_failure(
        USER,
        TASK,
        GenerationError(
            code="REJECTED", stage="PROVIDER", message="Original provider rejection"
        ),
    )
    with pytest.raises(StoreConflict):
        store.attach_provider_evidence(attached, "operator", ("byteplus-primary",))
    original = store.execution(USER, TASK)
    assert original.status == "FAILED"
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM provider_reconciliation_audit")
        assert cursor.fetchone()[0] == 0
    assert store.execution(USER, TASK) == original


def original_image_failure(kind):
    from lux_model_connector.byteplus import SeedreamReceipt

    model = "seedream-5-0-260128"
    if kind == "provider_error":
        return SeedreamReceipt(
            model=model, error={"code": "Blocked", "message": "Blocked"}
        )
    if kind == "image_error":
        return SeedreamReceipt(
            model=model, data=[{"error": {"code": "Blocked", "message": "Blocked"}}]
        )
    if kind == "multiple_images":
        return SeedreamReceipt(
            model=model,
            data=[
                {"url": "https://example.org/a.png"},
                {"url": "https://example.org/b.png"},
            ],
        )
    return SeedreamReceipt(
        model=model, data=[{"url": "https://example.org/a.png", "b64_json": "aW1hZ2U="}]
    )


async def normalized_image_setup(client, results):
    from test_byteplus_adapters import image_plan

    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import (
        NormalizedPreparedSubmission,
        decode_generation_request,
    )

    registry = byteplus_registry(client, results)
    request = decode_generation_request(image_plan(), "IMAGE")
    registration, adapter = registry.resolve(request)
    prepared = NormalizedPreparedSubmission(
        account_ref=registration.account_ref,
        kind="IMAGE",
        binding=request.provider_binding,
        request=request,
        provider_payload=await adapter.translate(request, ()),
    )
    return registry, adapter, prepared


@pytest.mark.parametrize(
    "kind", ["provider_error", "image_error", "multiple_images", "ambiguous_image"]
)
def test_original_image_normalization_matches_provider_submission(kind):
    import asyncio

    from test_execution import ClientDouble, ResultsDouble
    from test_store import OPERATION, SESSION

    from lux_model_connector.protocol import GenerationIdentity
    from lux_model_connector.reconciliation import original_image_receipt

    async def scenario():
        client, results = ClientDouble(), ResultsDouble()
        client.image_response = original_image_failure(kind)
        _, adapter, prepared = await normalized_image_setup(client, results)
        identity = GenerationIdentity(
            user_id=USER, session_id=SESSION, task_id=TASK, operation_id=OPERATION
        )
        normal = await adapter.submit(identity, prepared)
        attached = original_image_receipt(prepared, client.image_response)
        assert attached.model_dump(mode="json") == normal.model_dump(mode="json")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind", ["provider_error", "image_error", "multiple_images", "ambiguous_image"]
)
def test_pending_original_image_receipt_converges_with_attachment(store, kind):
    import asyncio

    from test_execution import ClientDouble, ResultsDouble
    from test_store import submit

    from lux_model_connector.execution import ProviderWorker
    from lux_model_connector.reconciliation import (
        OriginalProviderEvidence,
        original_view,
    )

    async def scenario():
        client, results = ClientDouble(), ResultsDouble()
        client.image_response = original_image_failure(kind)
        registry, _, prepared = await normalized_image_setup(client, results)
        store.receive(submit(request=prepared.request.model_dump(mode="json")))
        save_original = store.save_provider_receipt
        failures = 1

        def save_with_transient_failure(*args):
            nonlocal failures
            if failures:
                failures -= 1
                raise OSError("Original receipt database write unavailable")
            return save_original(*args)

        store.save_provider_receipt = save_with_transient_failure
        worker = ProviderWorker(store, client, results, adapters=registry)
        await worker.step()
        assert len(client.image_calls) == 1 and worker._pending is not None
        view = original_view(store.reconciliation_work(USER, TASK))
        evidence = OriginalProviderEvidence(
            binding=view.binding,
            image_receipt=client.image_response,
            evidence_reference="ticket:pending-original",
            reason="Same original response recovered while original write was pending",
        )
        store.attach_provider_evidence(evidence, "operator", ("byteplus-primary",))
        original = store.reconciliation_work(USER, TASK).provider_receipt
        await worker.step()
        assert worker._pending is None
        assert (
            len(client.image_calls) == 1
            and client.video_calls == []
            and client.query_calls == []
        )
        assert store.reconciliation_work(USER, TASK).provider_receipt == original
        assert store.execution(USER, TASK).status == "FAILED"
        assert results.outputs == []

    asyncio.run(scenario())
