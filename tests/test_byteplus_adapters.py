import asyncio
import base64
import hashlib
import io
import json
import struct
import zlib
from uuid import uuid4

import httpx
import pytest
from test_execution import ClientDouble, ResultsDouble, StoreDouble, initial_work
from test_generation_plan import request_fixture

from lux_model_connector.generation_plan import decode_generation_request


def image_plan(model="seedream-5.0-lite", format="PNG"):
    value = request_fixture()
    value["model_selection"] = {
        "model_id": model,
        "profile_id": "standard",
        "capability_revision": 1,
    }
    value["provider_binding"] = {
        "provider_id": "byteplus",
        "provider_model_id": {
            "seedream-4.5": "seedream-4-5-251128",
            "seedream-5.0-lite": "seedream-5-0-260128",
            "seedream-5.0-pro": "dola-seedream-5-0-pro-260628",
        }[model],
        "adapter_id": "byteplus-seedream-v1",
    }
    size = {"type": "pixel_size", "dimensions": {"width": 2048, "height": 2048}}
    for key in ("parameters", "result_expectations"):
        value[key]["size"] = size
        value[key]["output_format"] = format
    return value


def video_plan():
    value = image_plan()
    value["request_type"] = "video_generation_plan_v1"
    value["model_selection"]["model_id"] = "seedance-2.5"
    value["provider_binding"].update(
        provider_model_id="dreamina-seedance-2-5-260628",
        adapter_id="byteplus-seedance-v1",
    )
    size = {
        "type": "resolved_preset_size",
        "preset": {
            "preset_id": "720p-16:9",
            "aspect_ratio": {"width_units": 16, "height_units": 9},
            "resolution_id": "720p",
            "pixel_dimensions": {"width": 1280, "height": 720},
        },
    }
    value["parameters"].update(
        type="video_execution_parameters",
        size=size,
        generation_mode="VIDEO",
        output_format="MP4",
        duration_seconds=5,
        generate_audio=False,
    )
    value["result_expectations"].update(
        type="video_result_expectations",
        size=size,
        output_format="MP4",
        duration_seconds=5,
        duration_tolerance_milliseconds=500,
        expected_audio=False,
    )
    return value


def png(width=2048, height=2048):
    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress((b"\0" + b"\0" * width * 3) * height))
        + chunk(b"IEND", b"")
    )


def test_exact_routes_and_version_specific_translation():
    from lux_model_connector.byteplus_adapters import byteplus_registry

    async def run():
        registry = byteplus_registry(ClientDouble(), ResultsDouble())
        assert [
            (
                x.model_selection.model_id,
                x.model_selection.profile_id,
                x.model_selection.capability_revision,
            )
            for x in registry.registrations
        ] == [
            (x, "standard", 1)
            for x in (
                "seedream-4.5",
                "seedream-5.0-lite",
                "seedream-5.0-pro",
                "seedance-2.5",
            )
        ]
        for model in ("seedream-4.5", "seedream-5.0-lite", "seedream-5.0-pro"):
            request = decode_generation_request(image_plan(model, "JPEG"), "IMAGE")
            _, adapter = registry.resolve(request)
            translated = await adapter.translate(request, ())
            assert translated["size"] == "2048x2048"
            assert translated["output_format"] == "jpeg"
            assert translated["stream"] is False
            if model.endswith("pro"):
                assert translated["layer_decomposition"] is False
                assert "sequential_image_generation" not in translated
            else:
                assert translated["sequential_image_generation"] == "disabled"
                assert "layer_decomposition" not in translated
        request = decode_generation_request(image_plan("seedream-4.5"), "IMAGE")
        with pytest.raises(ValueError, match="format"):
            await registry.resolve(request)[1].translate(request, ())

    asyncio.run(run())


def test_video_id_persisted_before_poll_and_recovered_after_restart():
    from lux_model_connector.byteplus import SeedanceReceipt, VideoCreated
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.execution import ProviderWorker

    async def run():
        store = StoreDouble(initial_work("VIDEO", video_plan()))

        class Client(ClientDouble):
            async def create_video(self, request):
                self.video_calls.append(request)
                assert request.draft is False and request.return_last_frame is False
                assert request.omni_reference_task_type == "reference"
                return VideoCreated(id="original-task")

            async def get_video(self, external_id):
                assert (
                    store.work[1].provider_external_id == external_id == "original-task"
                )
                assert len(self.video_calls) == 1
                self.query_calls.append(external_id)
                return SeedanceReceipt(id=external_id, status="running")

        client, results = Client(), ResultsDouble()
        registry = byteplus_registry(client, results)
        worker = ProviderWorker(store, client, results, adapters=registry)
        store.receipt_failures = 1
        await worker.step()
        assert client.query_calls == []
        await worker.step()
        restarted = ProviderWorker(store, client, results, adapters=registry)
        await restarted.step()
        assert client.query_calls == ["original-task", "original-task"]
        assert len(client.video_calls) == 1

    asyncio.run(run())


def test_asset_locator_scopes_owner_and_checks_content():
    from lux_model_connector.asset_client import JavaAssetLocator
    from lux_model_connector.protocol import GenerationIdentity

    identity = GenerationIdentity(
        user_id=str(uuid4()),
        session_id=str(uuid4()),
        task_id=str(uuid4()),
        operation_id=str(uuid4()),
    )
    asset_id, body = str(uuid4()), png(32, 32)
    paths = []

    def respond(request):
        paths.append(request.url.path)
        prefix = f"/internal/v1/users/{identity.user_id}/assets/{asset_id}"
        if request.url.path == prefix:
            return httpx.Response(
                200,
                json={
                    "id": asset_id,
                    "user_id": identity.user_id,
                    "task_id": None,
                    "filename": "ref.png",
                    "media_type": "image/png",
                    "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "created_at": "2026-10-13T00:00:00Z",
                },
            )
        assert request.url.path == prefix + "/content"
        return httpx.Response(200, content=body, headers={"content-type": "image/png"})

    async def run():
        async with httpx.AsyncClient(
            base_url="http://java.local", transport=httpx.MockTransport(respond)
        ) as http:
            asset = await JavaAssetLocator(http).locate(identity, asset_id)
            assert (
                asset.url == "data:image/png;base64," + base64.b64encode(body).decode()
            )
            assert len(paths) == 2

    asyncio.run(run())


def test_retained_byte_verification_rejects_dimensions_and_checksum():
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.results import S3Object, StoredResult

    body = png(32, 32)

    class Results:
        def read(self, result):
            return S3Object(io.BytesIO(body), "image/png", len(body))

    result = StoredResult(
        source_index=0,
        store_ref="0" * 64,
        bucket="results",
        object_key="file",
        filename="0.png",
        media_type="image/png",
        sha256=hashlib.sha256(body).hexdigest(),
        byte_length=len(body),
    )

    async def run():
        request = decode_generation_request(image_plan(), "IMAGE")
        adapter = byteplus_registry(ClientDouble(), Results()).resolve(request)[1]
        with pytest.raises(ValueError, match="dimensions"):
            await adapter.verify_result(request, result)
        with pytest.raises(ValueError, match="checksum"):
            await adapter.verify_result(
                request, result.model_copy(update={"sha256": "0" * 64})
            )

    asyncio.run(run())


def test_http_translation_retains_image_and_never_reposts_uncertain_submission():
    from lux_model_connector.byteplus import BytePlusClient, BytePlusSettings
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.execution import ProviderWorker
    from lux_model_connector.results import S3Object

    async def run():
        posts = []
        image = png()

        class Results(ResultsDouble):
            async def preserve(self, user_id, task_id, output):
                saved = await super().preserve(user_id, task_id, output)
                return saved.model_copy(
                    update={
                        "byte_length": len(image),
                        "sha256": hashlib.sha256(image).hexdigest(),
                    }
                )

            def read(self, result):
                return S3Object(io.BytesIO(image), "image/png", len(image))

        def respond(request):
            posts.append(json.loads(request.content))
            return httpx.Response(
                200, json={"data": [{"b64_json": base64.b64encode(image).decode()}]}
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            client, results = (
                BytePlusClient(BytePlusSettings(api_key="fixture"), http),
                Results(),
            )
            store = StoreDouble(initial_work(request=image_plan()))
            worker = ProviderWorker(
                store, client, results, adapters=byteplus_registry(client, results)
            )
            await worker.step()
            await worker.step()
            assert store.work[1].execution.status == "SUCCEEDED"
            assert len(posts) == 1
            assert posts[0]["model"] == "seedream-5-0-260128"
            assert (
                store.prepared[0].provider_payload["model_id"] == "seedream-5-0-260128"
            )
        posts.clear()

        def uncertain(request):
            posts.append(request)
            raise httpx.ReadTimeout("lost receipt")

        async with httpx.AsyncClient(transport=httpx.MockTransport(uncertain)) as http:
            client = BytePlusClient(BytePlusSettings(api_key="fixture"), http)
            store = StoreDouble(initial_work(request=image_plan()))
            registry = byteplus_registry(client, Results())
            await ProviderWorker(store, client, Results(), adapters=registry).step()
            await ProviderWorker(store, client, Results(), adapters=registry).step()
            assert len(posts) == 1
            assert store.work[1].execution.status == "RUNNING"
            assert store.work[1].provider_receipt is None

    asyncio.run(run())


def test_lifecycle_registers_routes_and_scoped_locator_only_when_enabled(monkeypatch):
    from lux_model_connector.byteplus import BytePlusSettings
    from lux_model_connector.main import ServiceSettings, create_app
    from lux_model_connector.results import S3Settings
    from lux_model_connector.store import MySQLSettings

    class Store:
        def __init__(self, *args, **kwargs):
            pass

        def initialize(self):
            pass

        def validate_provider_capacity(self, *args):
            pass

    workers = []

    class Worker:
        def __init__(self, *args, **kwargs):
            workers.append(kwargs)

        async def run(self):
            await asyncio.Event().wait()

    monkeypatch.setattr("lux_model_connector.main.ConnectorStore", Store)
    monkeypatch.setattr("lux_model_connector.main.ProviderWorker", Worker)

    async def run():
        async with create_app(ServiceSettings()).router.lifespan_context(
            create_app(ServiceSettings())
        ):
            assert workers == []
        settings = ServiceSettings(
            mysql=MySQLSettings.from_dsn("mysql://fixture:fixture@localhost/lux"),
            byteplus=BytePlusSettings(api_key="fixture"),
            results=S3Settings(
                bucket="fixture",
                region_name="fixture",
                access_key_id="fixture",
                secret_access_key="fixture",
            ),
            result_base_url="http://connector.local",
            provider_execution_enabled=True,
            backend_url="http://java.local",
        )
        app = create_app(
            settings, provider_client=ClientDouble(), result_store=ResultsDouble()
        )
        async with app.router.lifespan_context(app):
            assert len(workers[0]["adapters"].registrations) == 4
            assert workers[0]["asset_locator"] is not None

    asyncio.run(run())


def test_original_provider_receipt_is_archived_without_changing_old_receipts():
    from lux_model_connector.byteplus import SeedreamReceipt
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import (
        NormalizedPreparedSubmission,
        NormalizedReceipt,
    )

    async def run():
        original = SeedreamReceipt.model_validate(
            {
                "model": "provider-revision",
                "data": [{"url": "https://provider.example/file.png"}],
                "usage": {"generated_images": 1},
            }
        )

        class Client(ClientDouble):
            async def generate_image(self, request):
                return original

        registry = byteplus_registry(Client(), ResultsDouble())
        request = decode_generation_request(image_plan(), "IMAGE")
        route, adapter = registry.resolve(request)
        prepared = NormalizedPreparedSubmission(
            account_ref=route.account_ref,
            kind="IMAGE",
            binding=route.binding,
            request=request,
            provider_payload=await adapter.translate(request, ()),
        )
        result = await adapter.submit(
            initial_work().execution.input_generation_payload.identity, prepared
        )
        assert result.original_image_receipt.model == original.model
        assert result.original_image_receipt.usage == original.usage
        assert (
            NormalizedReceipt.model_validate_json(
                result.model_dump_json()
            ).original_image_receipt
            == result.original_image_receipt
        )
        assert (
            NormalizedReceipt.model_validate(
                {"status": "FAILED"}
            ).original_image_receipt
            is None
        )

    asyncio.run(run())


@pytest.mark.parametrize("format", ["MP4", "MOV"])
def test_real_video_bytes_verify_duration_audio_container(format, tmp_path):
    import subprocess

    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.results import S3Object, StoredResult

    source = tmp_path / ("video.mp4" if format == "MP4" else "video.mov")
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=size=1280x720:rate=24",
            "-t",
            "5",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            str(source),
        ],
        check=True,
        timeout=20,
    )
    body = source.read_bytes()
    media_type = "video/mp4" if format == "MP4" else "video/quicktime"

    class Results:
        def read(self, result):
            return S3Object(io.BytesIO(body), media_type, len(body))

    result = StoredResult(
        source_index=0,
        store_ref="0" * 64,
        bucket="results",
        object_key="file",
        filename=source.name,
        media_type=media_type,
        sha256=hashlib.sha256(body).hexdigest(),
        byte_length=len(body),
    )

    async def run():
        value = video_plan()
        for key in ("parameters", "result_expectations"):
            value[key]["output_format"] = format
        request = decode_generation_request(value, "VIDEO")
        adapter = byteplus_registry(ClientDouble(), Results()).resolve(request)[1]
        await adapter.verify_result(request, result)
        value["parameters"]["generate_audio"] = True
        value["result_expectations"]["expected_audio"] = True
        with pytest.raises(ValueError, match="audio"):
            await adapter.verify_result(
                decode_generation_request(value, "VIDEO"), result
            )
        value["parameters"]["duration_seconds"] = 6
        value["result_expectations"]["duration_seconds"] = 6
        with pytest.raises(ValueError, match="duration"):
            await adapter.verify_result(
                decode_generation_request(value, "VIDEO"), result
            )

    asyncio.run(run())


@pytest.mark.parametrize("wrong", ["owner", "checksum", "mime", "oversize"])
def test_asset_locator_rejects_wrong_owner_integrity_format_and_budget(wrong):
    from lux_model_connector.asset_client import JavaAssetLocator

    identity = initial_work().execution.input_generation_payload.identity
    asset_id, body = str(uuid4()), png(32, 32)
    paths = []

    def respond(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/content"):
            return httpx.Response(
                200, content=body, headers={"content-type": "image/png"}
            )
        metadata = {
            "id": asset_id,
            "user_id": identity.user_id,
            "task_id": None,
            "filename": "ref.png",
            "media_type": "image/png",
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "created_at": "2026-10-13T00:00:00Z",
        }
        if wrong == "owner":
            metadata["user_id"] = str(uuid4())
        elif wrong == "checksum":
            metadata["sha256"] = "0" * 64
        elif wrong == "mime":
            metadata["media_type"] = "video/mp4"
        else:
            metadata["size_bytes"] = 11 * 1024 * 1024
        return httpx.Response(200, json=metadata)

    async def run():
        async with httpx.AsyncClient(
            base_url="http://java.local", transport=httpx.MockTransport(respond)
        ) as http:
            with pytest.raises(ValueError):
                await JavaAssetLocator(http).locate(identity, asset_id)
        assert len(paths) == (2 if wrong == "checksum" else 1)

    asyncio.run(run())


def test_prepared_account_and_provider_model_mismatch_make_no_provider_calls():
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import NormalizedPreparedSubmission

    async def run():
        client = ClientDouble()
        request = decode_generation_request(image_plan(), "IMAGE")
        route, adapter = byteplus_registry(client, ResultsDouble()).resolve(request)
        payload = await adapter.translate(request, ())
        prepared = NormalizedPreparedSubmission(
            account_ref="foreign",
            kind="IMAGE",
            binding=route.binding,
            request=request,
            provider_payload=payload,
        )
        identity = initial_work().execution.input_generation_payload.identity
        with pytest.raises(ValueError, match="account"):
            await adapter.submit(identity, prepared)
        payload["model_id"] = "foreign"
        prepared = prepared.model_copy(
            update={"account_ref": route.account_ref, "provider_payload": payload}
        )
        with pytest.raises(ValueError, match="model"):
            await adapter.submit(identity, prepared)
        assert client.image_calls == []

    asyncio.run(run())


def test_video_reference_dimensions_and_utf8_prompt_are_preflight_limits():
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import LocatedAsset, PixelDimensions

    async def run():
        asset_id = str(uuid4())
        value = video_plan()
        value["parameters"]["references"] = [
            {"type": "reference_asset", "asset_id": asset_id}
        ]
        request = decode_generation_request(value, "VIDEO")
        adapter = byteplus_registry(ClientDouble(), ResultsDouble()).resolve(request)[1]
        asset = LocatedAsset(
            asset_id=asset_id,
            media_type="image/png",
            url="data:image/png;base64,fixture",
            dimensions=PixelDimensions(width=299, height=500),
            size_bytes=1,
        )
        with pytest.raises(ValueError, match="dimensions"):
            await adapter.translate(request, (asset,))
        with pytest.raises(ValueError, match="ratio"):
            await adapter.translate(
                request,
                (
                    asset.model_copy(
                        update={"dimensions": PixelDimensions(width=300, height=1000)}
                    ),
                ),
            )
        await adapter.translate(
            request,
            (
                asset.model_copy(
                    update={"dimensions": PixelDimensions(width=300, height=500)}
                ),
            ),
        )
        value = image_plan()
        value["parameters"]["prompt"] = "图" * 1334
        with pytest.raises(ValueError, match="prompt"):
            await adapter.translate(decode_generation_request(value, "IMAGE"), ())

    asyncio.run(run())


def test_corrupt_png_bytes_are_rejected_for_asset_input_and_retained_output():
    from lux_model_connector.asset_client import JavaAssetLocator
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.results import S3Object, StoredResult

    original = png()
    payload = b"garbage" * 1000
    body = (
        original[:33]
        + struct.pack(">I", len(payload))
        + b"IDAT"
        + payload
        + struct.pack(">I", zlib.crc32(b"IDAT" + payload))
        + original[-12:]
    )
    identity = initial_work().execution.input_generation_payload.identity
    asset_id = str(uuid4())

    class Results:
        def read(self, result):
            return S3Object(io.BytesIO(body), "image/png", len(body))

    result = StoredResult(
        source_index=0,
        store_ref="0" * 64,
        bucket="results",
        object_key="file",
        filename="file.png",
        media_type="image/png",
        sha256=hashlib.sha256(body).hexdigest(),
        byte_length=len(body),
    )

    def respond(request):
        if request.url.path.endswith("/content"):
            return httpx.Response(
                200, content=body, headers={"content-type": "image/png"}
            )
        return httpx.Response(
            200,
            json={
                "id": asset_id,
                "user_id": identity.user_id,
                "task_id": None,
                "filename": "ref.png",
                "media_type": "image/png",
                "size_bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "created_at": "2026-10-13T00:00:00Z",
            },
        )

    async def run():
        async with httpx.AsyncClient(
            base_url="http://java.local", transport=httpx.MockTransport(respond)
        ) as http:
            with pytest.raises(ValueError):
                await JavaAssetLocator(http).locate(identity, asset_id)
        request = decode_generation_request(image_plan(), "IMAGE")
        adapter = byteplus_registry(ClientDouble(), Results()).resolve(request)[1]
        with pytest.raises(ValueError):
            await adapter.verify_result(request, result)

    asyncio.run(run())


def test_video_headers_without_decodable_samples_are_rejected(tmp_path):
    import subprocess

    from lux_model_connector.media_inspection import inspect_media

    source = tmp_path / "video.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=size=1280x720:rate=24",
            "-t",
            "5",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-movflags",
            "+faststart",
            str(source),
        ],
        check=True,
        timeout=20,
    )
    original = source.read_bytes()
    boxes = bytearray()
    offset = 0
    while offset < len(original):
        length, kind = struct.unpack(">I4s", original[offset : offset + 8])
        assert length >= 8
        boxes.extend(
            struct.pack(">I4s", 8, b"mdat")
            if kind == b"mdat"
            else original[offset : offset + length]
        )
        offset += length
    with pytest.raises(ValueError):
        inspect_media(bytes(boxes), "video/mp4")


@pytest.mark.parametrize(
    "model,maximum",
    [
        ("seedream-4.5", 3 * 1024 * 1024),
        ("seedream-5.0-lite", 3 * 1024 * 1024),
        ("seedream-5.0-pro", 4 * 1024 * 1024),
        ("seedance-2.5", 3 * 1024 * 1024 // 2),
    ],
)
def test_model_specific_reference_byte_caps(model, maximum):
    from lux_model_connector.byteplus_adapters import byteplus_registry
    from lux_model_connector.generation_plan import LocatedAsset, PixelDimensions

    async def run():
        value = video_plan() if model == "seedance-2.5" else image_plan(model, "JPEG")
        asset_id = str(uuid4())
        value["parameters"]["references"] = [
            {"type": "reference_asset", "asset_id": asset_id}
        ]
        request = decode_generation_request(
            value, "VIDEO" if model == "seedance-2.5" else "IMAGE"
        )
        adapter = byteplus_registry(ClientDouble(), ResultsDouble()).resolve(request)[1]
        asset = LocatedAsset(
            asset_id=asset_id,
            media_type="image/png",
            url="data:image/png;base64,fixture",
            dimensions=PixelDimensions(width=300, height=500),
            size_bytes=maximum + 1,
        )
        with pytest.raises(ValueError, match="byte"):
            await adapter.translate(request, (asset,))
        await adapter.translate(
            request, (asset.model_copy(update={"size_bytes": maximum}),)
        )

    asyncio.run(run())
