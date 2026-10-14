import pytest

from lux_model_connector.generation_plan import (
    AdapterRegistry,
    decode_generation_request,
)


def test_recognized_invalid_request_never_legacy_falls_back():
    with pytest.raises(ValueError):
        decode_generation_request({"request_type": "image_generation_plan_v1"}, "IMAGE")


def test_empty_registry_rejects_unknown_route():
    assert AdapterRegistry().registrations == ()


def request_fixture():
    return {
        "request_type": "image_generation_plan_v1",
        "schema_version": 1,
        "model_selection": {
            "model_id": "local",
            "profile_id": "image",
            "capability_revision": 1,
        },
        "provider_binding": {
            "provider_id": "offline",
            "provider_model_id": "local-image",
            "adapter_id": "local-v1",
        },
        "parameters": {
            "type": "image_execution_parameters",
            "prompt": "hello",
            "size": {"type": "pixel_size", "dimensions": {"width": 32, "height": 32}},
            "references": [],
            "output_format": "PNG",
            "generation_mode": "IMAGE",
        },
        "result_expectations": {
            "type": "image_result_expectations",
            "file_count": 1,
            "size": {"type": "pixel_size", "dimensions": {"width": 32, "height": 32}},
            "output_format": "PNG",
        },
    }


@pytest.mark.parametrize(
    "field,value",
    [("schema_version", True), ("request_type", "future_plan"), ("extra", 1)],
)
def test_closed_contract(field, value):
    request = request_fixture()
    request[field] = value
    with pytest.raises(ValueError):
        decode_generation_request(request, "IMAGE")


def test_registered_worker_persists_original_before_dispatch_and_verifies():
    import asyncio

    from test_execution import ClientDouble, ResultsDouble, StoreDouble, initial_work

    from lux_model_connector.execution import ProviderWorker
    from lux_model_connector.generation_plan import (
        AdapterRegistration,
        NormalizedReceipt,
    )
    from lux_model_connector.results import PreparedOutput

    async def scenario():
        request = decode_generation_request(request_fixture(), "IMAGE")
        work = initial_work(request=request_fixture())
        store, client, results = StoreDouble(work), ClientDouble(), ResultsDouble()

        class Adapter:
            calls = 0
            verified = 0

            async def translate(self, request, assets):
                assert assets == ()
                return {
                    "count": 1,
                    "auxiliary": False,
                    "prompt": request.parameters.prompt,
                }

            async def submit(self, identity, prepared):
                self.calls += 1
                persisted = store.work[1]
                assert persisted.submission_started_at is not None
                assert persisted.submission_payload == prepared
                return NormalizedReceipt(
                    status="SUCCEEDED",
                    outputs=(
                        PreparedOutput(
                            source_index=0,
                            media_kind="IMAGE",
                            url="https://offline.example/output.png",
                        ),
                    ),
                )

            async def reconcile(self, identity, prepared):
                raise AssertionError("known receipt must be reused")

            async def verify_result(self, request, result):
                assert result.media_type == "image/png"
                self.verified += 1

        adapter = Adapter()
        registry = AdapterRegistry()
        registry.register(
            AdapterRegistration(
                binding=request.provider_binding,
                account_ref="offline-account",
                media_type="IMAGE",
                model_selection=request.model_selection,
                single_file=True,
                auxiliary_outputs_disabled=True,
                result_verification=True,
            ),
            adapter,
        )
        worker = ProviderWorker(store, client, results, adapters=registry)
        await worker.step()
        await worker.step()
        assert adapter.calls == 1
        assert adapter.verified == 1
        assert store.work[1].execution.status == "SUCCEEDED"
        assert client.image_calls == []

    asyncio.run(scenario())


def test_unregistered_normalized_route_rejected_before_dispatch():
    import asyncio

    from test_execution import worker_parts

    async def scenario():
        worker, store, client, results = worker_parts(request=request_fixture())
        await worker.step()
        assert store.work[1].execution.status == "FAILED"
        assert store.prepared == []
        assert client.image_calls == []

    asyncio.run(scenario())


def test_unknown_original_reconciliation_never_repeats_dispatch():
    import asyncio
    from dataclasses import replace
    from datetime import UTC, datetime

    from test_execution import ClientDouble, ResultsDouble, StoreDouble, initial_work

    from lux_model_connector.execution import ProviderWorker
    from lux_model_connector.generation_plan import (
        AdapterRegistration,
        NormalizedPreparedSubmission,
    )

    async def scenario():
        request = decode_generation_request(request_fixture(), "IMAGE")
        prepared = NormalizedPreparedSubmission(
            account_ref="offline",
            kind="IMAGE",
            binding=request.provider_binding,
            request=request,
            provider_payload={"original": True},
        )
        work = initial_work(request=request_fixture())
        work = replace(
            work,
            execution=replace(work.execution, status="RUNNING"),
            submission_payload=prepared,
            submission_started_at=datetime.now(UTC),
        )
        store = StoreDouble(work)

        class Adapter:
            calls = []

            async def reconcile(self, identity, payload, external_id, receipt):
                self.calls.append((identity, payload, external_id, receipt))
                return None

            async def submit(self, *args):
                raise AssertionError("uncertain original must never be resubmitted")

        adapter = Adapter()
        registry = AdapterRegistry()
        registry.register(
            AdapterRegistration(
                binding=request.provider_binding,
                account_ref="offline",
                media_type="IMAGE",
                model_selection=request.model_selection,
                single_file=True,
                auxiliary_outputs_disabled=True,
                result_verification=True,
            ),
            adapter,
        )
        worker = ProviderWorker(
            store, ClientDouble(), ResultsDouble(), adapters=registry
        )
        await worker._observe(work)
        await worker._observe(work)
        assert (
            adapter.calls
            == [
                (work.execution.input_generation_payload.identity, prepared, None, None)
            ]
            * 2
        )
        assert store.prepared == []

    asyncio.run(scenario())


def test_foreign_prepared_routes_do_not_starve_later_eligible_work():
    import asyncio
    from dataclasses import replace

    from test_execution import ClientDouble, ResultsDouble, StoreDouble, initial_work

    from lux_model_connector.byteplus import ProviderSubmissionPayload, SeedreamRequest
    from lux_model_connector.execution import ProviderWorker
    from lux_model_connector.generation_plan import (
        AdapterRegistration,
        NormalizedPreparedSubmission,
    )

    async def scenario():
        request = decode_generation_request(request_fixture(), "IMAGE")
        foreign_legacy = initial_work(execution_id=1)
        foreign_legacy = replace(
            foreign_legacy,
            submission_payload=ProviderSubmissionPayload(
                account_ref="foreign-legacy",
                kind="IMAGE",
                request=SeedreamRequest(model_id="legacy", prompt="x"),
            ),
        )
        foreign_normalized = initial_work(request=request_fixture(), execution_id=2)
        foreign_normalized = replace(
            foreign_normalized,
            submission_payload=NormalizedPreparedSubmission(
                account_ref="foreign-normalized",
                kind="IMAGE",
                binding=request.provider_binding,
                request=request,
                provider_payload={"count": 1},
            ),
        )
        eligible = initial_work(execution_id=3)

        class WildcardStore(StoreDouble):
            def claim_execution(self, *, account_ref=None, normalized_routes=()):
                # Model real SQL NULL wildcard semantics and the proposed eligibility.
                if account_ref is None and not normalized_routes:
                    return super().claim_execution(account_ref=None)
                return super().claim_execution(
                    account_ref=account_ref, normalized_routes=normalized_routes
                )

        store, client, results = (
            WildcardStore(foreign_legacy, foreign_normalized, eligible),
            ClientDouble(),
            ResultsDouble(),
        )
        registry = AdapterRegistry()
        registry.register(
            AdapterRegistration(
                binding=request.provider_binding,
                account_ref="eligible-normalized",
                media_type="IMAGE",
                model_selection=request.model_selection,
                single_file=True,
                auxiliary_outputs_disabled=True,
                result_verification=True,
            ),
            object(),
        )
        worker = ProviderWorker(store, client, results, adapters=registry)
        for _ in range(3):
            await worker.step()
        assert len(client.image_calls) == 1
        assert store.work[3].execution.status == "SUCCEEDED"
        assert store.work[1].submission_payload == foreign_legacy.submission_payload
        assert store.work[2].submission_payload == foreign_normalized.submission_payload
        assert store.work[1].submission_started_at is None
        assert store.work[2].submission_started_at is None

    asyncio.run(scenario())


def test_routing_selector_has_one_immutable_account():
    from lux_model_connector.generation_plan import AdapterRegistration

    request = decode_generation_request(request_fixture(), "IMAGE")
    first = AdapterRegistration(
        binding=request.provider_binding,
        account_ref="account-a",
        media_type="IMAGE",
        model_selection=request.model_selection,
        single_file=True,
        auxiliary_outputs_disabled=True,
        result_verification=True,
    )
    registry = AdapterRegistry()
    registry.register(first, object())
    second = first.model_copy(update={"account_ref": "account-b"})
    with pytest.raises(ValueError, match="Duplicate"):
        registry.register(second, object())
    assert registry.registrations == (first,)
    assert registry.resolve(request)[0].account_ref == "account-a"


@pytest.mark.parametrize("field", ["provider_id", "provider_model_id", "adapter_id"])
@pytest.mark.parametrize(
    "whitespace",
    [" ", "\t\n", "\u2003", "\u00a0", "\u0085", "\u2007", "\u202f", "\u3000"],
)
def test_provider_binding_rejects_blank_fields(field, whitespace):
    request = request_fixture()
    request["provider_binding"][field] = whitespace
    with pytest.raises(ValueError):
        decode_generation_request(request, "IMAGE")


def test_provider_binding_preserves_nonblank_exact_values():
    request = request_fixture()
    request["provider_binding"]["adapter_id"] = " adapter-v1 "
    assert (
        decode_generation_request(request, "IMAGE").provider_binding.adapter_id
        == " adapter-v1 "
    )
